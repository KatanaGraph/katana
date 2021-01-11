"""
Simple functions for calling commands as subprocesses.
"""
import logging
import signal
import subprocess

__all__ = ["capture_command", "predicate_command", "action_command", "CommandError"]

from pprint import pprint

logger = logging.getLogger(__name__)


class CommandError(subprocess.CalledProcessError):
    def __init__(self, *args):
        if len(args) == 1 and isinstance(args[0], subprocess.CalledProcessError):
            super(CommandError, self).__init__(args[0].returncode, args[0].cmd, args[0].output, args[0].stderr)
        else:
            super(CommandError, self).__init__(*args)

    def __str__(self):
        cmd = " ".join(repr(s) if "\n" in s else str(s) for s in self.cmd)
        output_str = ""
        if self.stdout:
            output_str += f"\nOutput:\n{self.stdout.decode()}"
        if self.stderr:
            output_str += f"\nError:\n{self.stderr.decode()}"
        if self.returncode and self.returncode < 0:
            try:
                return f"Command '{cmd}' died with {signal.Signals(-self.returncode)}.{output_str}"
            except ValueError:
                return f"Command '{cmd}' died with {-self.returncode}.{output_str}"
        else:
            return f"Command '{cmd}' returned non-zero exit status {self.returncode}.{output_str}"


def capture_command(*args, **kwargs) -> str:
    try:
        res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True, **kwargs)
    except subprocess.CalledProcessError as e:
        raise CommandError(e)
    out = res.stdout.decode("utf-8").strip("\n")
    logger.debug("capture $ %s -> %d >%s", " ".join(repr(s) for s in args), res.returncode, f"\n{out}" if out else "")
    return out


def predicate_command(*args, ignore_error=False, **kwargs) -> bool:
    try:
        res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
    except subprocess.CalledProcessError as e:
        raise CommandError(e)
    out = res.stdout.decode("utf-8").strip("\n")
    logger.debug("boolean $ %s -> %d >%s", " ".join(repr(s) for s in args), res.returncode, f"\n{out}" if out else "")
    if not ignore_error and res.returncode > 1:
        raise CommandError(res.returncode, res.args, res.stdout, res.stderr)
    return res.returncode == 0


def action_command(*args, dry_run=False, log=True, **kwargs) -> None:
    if log or dry_run:
        print("GIT: " + " ".join(repr(s) if "\n" in s else str(s) for s in args))
    if dry_run:
        return

    try:
        res = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)
    except subprocess.CalledProcessError as e:
        raise CommandError(e)
    out = res.stdout.decode("utf-8").strip("\n")
    logger.debug("action $ %s -> %d >%s", " ".join(repr(s) for s in args), res.returncode, f"\n{out}" if out else "")
    if res.returncode:
        raise CommandError(res.returncode, res.args, res.stdout, res.stderr)
