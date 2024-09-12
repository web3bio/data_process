#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import datetime
import codecs
import time
import os
import sys
import traceback

from logging import Handler
import setting

# sys.version > '3'
import io as cStringIO
try:
    unicode
    _unicode = True
except NameError:
    _unicode = False


class FileHandler(Handler):
    """
    Writes formatted logging records
    """
    def __init__(self, config):
        logging.Handler.__init__(self)

        self.log_path = "./log"
        self.log_level = 5
        self.log_max_size = 1600 * 1024 * 1024
        self.encoding = "utf8"
        self.server_name = None
        self.printstd = False

        if config is not None:
            try:
                self.log_path = config["server"]["log_path"]
            except:
                pass

            try:
                self.log_level = config["server"]["log_level"]
            except:
                pass

            try:
                self.log_max_size = config["server"]["log_max_size"]
            except:
                pass

            try:
                self.server_name = config["server"]["server_name"]
            except:
                pass

        # OFF=1, PANIC=2, ERROR=3, WARN = 4, DEBUG=5
        if self.log_level == 5:
            self.setLevel(logging.DEBUG)
        elif self.log_level == 4:
            self.setLevel(logging.WARNING)
        elif self.log_level == 3:
            self.setLevel(logging.INFO)
        elif self.log_level == 0:
            self.setLevel(logging.NOTSET)
        else:
            self.setLevel(logging.ERROR)

        self.last_file_time = ""
        self.stream = None
        self.log_size = 0

        self._open()

    def setPrintStd(self):
        """
        Print at terminal.
        """
        self.printstd = True

    def flush(self):
        """
        Flushes the stream.
        """
        self.acquire()
        try:
            if self.stream and hasattr(self.stream, "flush"):
                self.stream.flush()
        finally:
            self.release()

    def close(self):
        """
        Closes the stream.
        """
        self.acquire()
        try:
            try:
                if self.stream:
                    try:
                        self.flush()
                    finally:
                        stream = self.stream
                        self.stream = None
                        if hasattr(stream, "close"):
                            stream.close()
            finally:
                # Issue #19523: call unconditionally to
                # prevent a handler leak when delay is set
                logging.Handler.close(self)
        finally:
            self.release()

    def _open(self):
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """
        if self.server_name is None:
            return
        file_time = datetime.datetime.now().strftime("%Y%m%d%H")

        if file_time != self.last_file_time:
            self.acquire()
            try:
                file_time = datetime.datetime.now().strftime("%Y%m%d%H")
                if file_time != self.last_file_time:
                    if self.stream:
                        if hasattr(self.stream, "flush"):
                            self.stream.flush()
                        if hasattr(self.stream, "close"):
                            self.stream.close()
                        self.stream = None

                    if len(self.server_name) > 0 and self.log_path.find(self.server_name) > 0:
                        fn = "%s/%s.log" % (self.log_path, file_time)
                    else:
                        fn = "%s/%s_%s.log" % (self.log_path, self.server_name, file_time)
                    
                    self.stream = codecs.open(fn, "a", self.encoding)
            finally:
                self.release()
        
    def emit(self, record):
        """
        Emit a record.

        If the stream was not opened because 'delay' was specified in the
        constructor, open it before calling the superclass's emit.
        """
        if self.log_level == 0:
            return
        self._open()
        
        if not self.printstd and self.log_size > self.log_max_size:
            return
        
        try:
            msg = self.format(record)
            stream = self.stream
            fs = "%s\n"
            if not _unicode:
                msg = fs % msg
            else:
                if (isinstance(msg, unicode) and getattr(stream, 'encoding', None)):
                    ufs = u'%s\n'
                    msg = ufs % msg
                else:
                    msg = fs % msg
                    if getattr(stream, 'encoding', None):
                        msg = msg.decode(stream.encodin)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)
            return

        if self.printstd:
            try:
                sys.stderr.write(msg)
            except:
                pass
        
        if self.stream and self.log_size + len(msg) < self.log_max_size:
            try:
                if msg[-1:] != "\n":
                    msg = msg + "\n"
            except:
                pass
            
            self.stream.write(msg)
            self.flush()
            self.log_size += len(msg)


class Formatter():
    """Formatter logging format defines"""
    def __init__(self, log_name="root"):
        self.__log_name = log_name
    
    def set_log_name(self, log_name):
        self.__log_name = log_name

    def format_exception(self, ei):
        """
        Format and return the specified exception information as a string.

        This default implementation just uses
        traceback.print_exception()
        """
        sio = cStringIO.StringIO()
        traceback.print_exception(ei[0], ei[1], ei[2], None, sio)
        s = sio.getvalue()
        sio.close()
        if s[-1:] == "\n":
            s = s[:-1]
        return s

    def format(self, record):
        """
        Format log record.
        """
        ct = time.localtime(record.created)
        t = time.strftime("%Y-%m-%d %H:%M:%S", ct)

        record.message = record.getMessage()

        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.format_exception(record.exc_info)

        s = "%s [%s] [%d:%d] [%s:%d] " % (t, self.__log_name, record.process, record.thread, record.filename, record.lineno)
        e = ""

        if record.levelno == logging.DEBUG:
            s += "\033[32m[DEBUG] "
            e = " \033[0m"
        elif record.levelno == logging.INFO:
            s += "\033[1;35m[INFO] "
            e = " \033[0m"
        elif record.levelno == logging.WARNING:
            s += "\033[1;33m[WARN] "
            e = " \033[0m"
        elif record.levelno == logging.ERROR or record.levelno == logging.CRITICAL:
            s += "\033[1;31m[ERROR] "
            e = " \033[0m"

        s += record.message

        if record.exc_text:
            try:
                s = s + record.exc_text
            except UnicodeError:
                # Sometimes filenames have non-ASCII chars, which can lead
                # to errors when s is Unicode and record.exc_text is str
                # See issue 8924.
                # We also use replace for when there are multiple
                # encodings, e.g. UTF-8 for the filesystem and latin-1
                # for a script. See issue 13232.
                s = s + record.exc_text.decode(sys.getfilesystemencoding(),
                                               'replace')
        return s + e


# As long as it is imported, it will be initialized to output to stdout.
static_file_handler = FileHandler(None)
static_file_handler.setFormatter(Formatter())
static_file_handler.setPrintStd()

logger = logging.getLogger()
logger.addHandler(static_file_handler)
logger.setLevel(static_file_handler.level)


def InitLogger(config):
    """initialize logger
    """
    global static_file_handler, logger
    if static_file_handler is not None:
        logger.removeHandler(static_file_handler)
        static_file_handler.close()
        static_file_handler = None

    if setting.Settings["env"] == "development":
        static_file_handler = FileHandler(config)
        static_file_handler.setLevel(logging.DEBUG)
        static_file_handler.setPrintStd()
        static_file_handler.setFormatter(Formatter())

        logger.addHandler(static_file_handler)
        logger.setLevel(static_file_handler.level)
    else:
        static_file_handler = FileHandler(config)
        static_file_handler.setLevel(logging.DEBUG)
        static_file_handler.setPrintStd()
        static_file_handler.setFormatter(Formatter())

        logger.addHandler(static_file_handler)
        logger.setLevel(static_file_handler.level)


def SetLoggerName(name):
    fmt = Formatter()
    fmt.set_log_name(name)
    static_file_handler.setFormatter(fmt)