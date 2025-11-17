import datetime
import sys


class Logger:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    CYAN = "\033[96m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    GREEN = "\033[92m"

    def __init__(self, file_path: str|None = None, level:str="INFO"):
        self.file_path = file_path
        self.level = level

    def _format_console(self, level: str, msg: str):
        if level == "INFO":
            if self.level!="INFO":
                return None
            lvl = f"{self.BOLD}{self.CYAN}{level}{self.RESET}"
        elif level == "OK":
            if self.level=="WARN" or self.level=="ERROR":
                return None
            lvl = f"{self.BOLD}{self.GREEN}{'OK'}{self.RESET}"
        elif level == "WARN":
            if self.level=="ERROR":
                return None
            lvl = f"{self.BOLD}{self.YELLOW}{level}{self.RESET}"
        else:  # ERROR
            lvl = f"{self.BOLD}{self.RED}{level}{self.RESET}"

        # datetime.datetime.now().isoformat(timespec="seconds")
        # ts = f"{self.DIM}{timestamp}{self.RESET}"

        return f"{lvl} {msg}"

    def _format_file(self, level: str, msg: str):
        timestamp = datetime.datetime.now().isoformat(timespec="seconds")
        return f"{timestamp} [{level}] {msg}\n"

    def _log(self, level: str, msg: str):
        console_line = self._format_console(level, msg)
        if not console_line:
            return
        print(console_line, file=sys.stdout)
        if self.file_path is not None:
            file_line = self._format_file(level, msg)
            with open(self.file_path, "a") as f:
                f.write(file_line)
        if level=="ERROR":
            raise Exception(msg)

    def ok(self, msg: str):
        self._log("OK", msg)

    def info(self, msg: str):
        self._log("INFO", msg)

    def warn(self, msg: str):
        self._log("WARN", msg)

    def error(self, msg: str):
        self._log("ERROR", msg)
