"""Local hardware evidence, disk budgets and an OS-released campaign lock."""
import ctypes
import hashlib
import os
import platform
import shutil
import socket
import sys
from contextlib import contextmanager
from pathlib import Path


def hardware():
    ram, cpu = None, platform.processor() or None
    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong),
                        *[(name, ctypes.c_ulonglong) for name in ("total", "available", "page_total", "page_available", "virtual_total", "virtual_available", "extended")]]
        info = MemoryStatus()
        info.length = ctypes.sizeof(info)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(info)):
            ram = info.total
        try:
            import winreg
            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0") as key:
                cpu = winreg.QueryValueEx(key, "ProcessorNameString")[0].strip()
        except OSError:
            pass
    else:
        try:
            ram = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
        except (ValueError, OSError, AttributeError):
            pass
    return {"hostname": socket.gethostname(), "cpu": cpu, "logical_cpu_count": os.cpu_count(),
            "ram_total_bytes": ram, "operating_system": platform.platform(), "machine": platform.machine(),
            "python_version": sys.version, "python_executable": sys.executable,
            "inherited_jvm_options_sha256": {name: hashlib.sha256(os.environ[name].encode()).hexdigest()
                                               for name in ("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS") if name in os.environ},
            "unavailable_fields": [name for name, value in (("ram_total_bytes", ram), ("cpu", cpu)) if value is None]}


def disk_budget(path: Path, reserve: int, estimated: int, repetitions: int):
    probe = path.resolve()
    while not probe.exists():
        probe = probe.parent
    usage = shutil.disk_usage(probe)
    required = reserve + estimated * repetitions
    return {"probe_path": str(probe), "free_bytes": usage.free, "required_bytes": required,
            "reserve_free_bytes": reserve, "estimated_bytes_per_repetition": estimated,
            "remaining_repetitions": repetitions, "sufficient": usage.free >= required,
            "kind": "operator_supplied_budget_not_measured_prediction"}


@contextmanager
def campaign_lock(path: Path):
    with path.open("a+b") as handle:
        if handle.seek(0, 2) == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise RuntimeError("Another runner owns this campaign; no concurrent execution is allowed") from exc
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle, fcntl.LOCK_UN)


def pid_alive(pid):
    if os.name == "nt":
        kernel = ctypes.windll.kernel32
        kernel.OpenProcess.restype = ctypes.c_void_p
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            error = kernel.GetLastError()
            if error not in (0, 87, 1168):
                raise RuntimeError("Cannot determine whether the previous child PID is still active")
            return False
        code = ctypes.c_ulong()
        try:
            return bool(kernel.GetExitCodeProcess(ctypes.c_void_p(handle), ctypes.byref(code))) and code.value == 259
        finally:
            kernel.CloseHandle(ctypes.c_void_p(handle))
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
