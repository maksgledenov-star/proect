import subprocess
from abc import abstractmethod, ABC
from pathlib import Path

from core.utils.exceptions import TelegramBotNotificationException


class BaseNotificationSender(ABC):
    @abstractmethod
    def send(self, message: str):
        pass


class TelegramBotNotificationSender(BaseNotificationSender):
    def __init__(self, path: Path | None):
        self.path = self._check_path(path)

    @staticmethod
    def _check_path(path):
        if not path.exists():
            raise TelegramBotNotificationException(f"Path not found: {path}")
        return path

    @staticmethod
    def _escape_for_cmd_single_arg(s: str) -> str:
        # Only escape %, leave quoting to subprocess (safer)
        # Optionally flatten newlines which BATs may mishandle:
        s = s.replace("\r\n", "\n").replace("\r", "\n").replace("\n", " ")
        return s.replace('%', '%%')

    def send(self, message: str) -> None | Exception:

        safe_msg = self._escape_for_cmd_single_arg(message)

        bat_file = self.path
        bot_dir = bat_file.parent

        cmd = ["cmd.exe", "/c", str(bat_file), safe_msg]

        try:
            subprocess.run(
                cmd,
                cwd=str(bot_dir),
                check=True,
                text=True,
                capture_output=True,
                timeout=20
            )

        except subprocess.TimeoutExpired:
            raise TelegramBotNotificationException("Telegram bot notification timed out.")
        except subprocess.CalledProcessError as e:
            # Surface real error from the BAT for debugging
            raise TelegramBotNotificationException(
                f"Notifier BAT failed (exit {e.returncode}).\n"
                f"CMD: {cmd}\n"
                f"CWD: {bot_dir}\n"
                f"STDOUT:\n{e.stdout}\n"
                f"STDERR:\n{e.stderr}"
            ) from e
