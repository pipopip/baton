"""Artifact capture policy and bounded streaming primitives for compute jobs.

Policy is persisted with each job so a subsequent controller cannot silently
change that job's limits.  Missing policies identify records created before v2.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import stat
import contextlib

MIB = 1024 * 1024
DEFAULT_FILE = 256 * MIB
DEFAULT_TOTAL = 1024 * MIB
MAX_FILE = 1024 * MIB
MAX_TOTAL = 4096 * MIB
LEGACY_FILE = 8 * MIB
CHUNK = 256 * 1024
MAX_HEADER = 4096
DISK_RESERVE = 2 * 1024 * MIB


def normalize_policy(req):
    """Validate user-facing MiB limits and return the pinned v2 policy."""
    def amount(name, default, maximum):
        value = req.get(name, default // MIB)
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum // MIB:
            raise ValueError(f"{name} must be an integer from 1 to {maximum // MIB} MiB")
        return value * MIB
    maximum = amount('artifact_max_mb', DEFAULT_FILE, MAX_FILE)
    total = amount('artifact_total_mb', DEFAULT_TOTAL, MAX_TOTAL)
    if maximum > total:
        raise ValueError('artifact_max_mb cannot exceed artifact_total_mb')
    return {'version': 2, 'max_file_bytes': maximum, 'total_bytes': total}


def validate_policy(policy):
    """Return a checked policy; never repair malformed persisted limits."""
    if not isinstance(policy, dict) or set(policy) != {'version', 'max_file_bytes', 'total_bytes'}:
        raise ValueError('invalid artifact policy')
    version, maximum, total = (policy[k] for k in ('version', 'max_file_bytes', 'total_bytes'))
    if (type(version) is not int or version != 2 or type(maximum) is not int or type(total) is not int
            or not 1 <= maximum <= MAX_FILE or not maximum <= total <= MAX_TOTAL):
        raise ValueError('invalid artifact policy')
    return dict(policy)


def _components(relative):
    if not isinstance(relative, str) or not relative or '\\' in relative or '\x00' in relative:
        raise ValueError('invalid artifact path')
    parts = relative.split('/')
    if any(p in ('', '.', '..') or ':' in p for p in parts):
        raise ValueError('invalid artifact path')
    return parts


def _open_directory_posix(path, own=False):
    """Open a root directory so that nothing below it can be reached through a link.

    The path leading to a root is the controller's own and may hold links (macOS keeps temporary directories and
    the like behind ``/var`` -> ``/private/var``): it is resolved once here and the resolved path is walked without
    following links. What is resolved depends on whose directory the root is. ``own``: the controller's state
    directory, which may itself be a link to another disk, is resolved whole. Otherwise the root is a per-job
    directory (``<state>/run/<job>/src``, ``~/q/<job>/src``) that a job can replace, so only its parent is resolved
    and a link in the root's own place is refused. Every link below either kind of root is refused (``O_NOFOLLOW``
    on each component here and in every later open).
    """
    absolute = os.path.abspath(path)
    if own:
        absolute = os.path.realpath(absolute)
    else:
        absolute = os.path.join(os.path.realpath(os.path.dirname(absolute)), os.path.basename(absolute))
    fd = os.open(os.path.sep, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in absolute.split(os.path.sep):
            if not part:
                continue
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


def open_source_posix(root, relative):
    """Open a regular source without following links in any path component."""
    if not hasattr(os, 'O_NOFOLLOW') or not hasattr(os, 'O_DIRECTORY'):
        raise OSError('safe no-follow source open unavailable')
    parts = _components(relative)
    dirs = []
    try:
        fd = _open_directory_posix(root)
        dirs.append(fd)
        for part in parts[:-1]:
            fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            dirs.append(fd)
        fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            os.close(fd)
            raise OSError('artifact is not a regular file')
        return os.fdopen(fd, 'rb'), info
    finally:
        for directory in reversed(dirs):
            os.close(directory)


def _windows_long_path(path):
    """Expand DOS short names without resolving junctions or symbolic links."""
    import ctypes
    from ctypes import wintypes
    api = ctypes.WinDLL('kernel32', use_last_error=True).GetLongPathNameW
    api.argtypes = [wintypes.LPCWSTR, wintypes.LPWSTR, wintypes.DWORD]
    api.restype = wintypes.DWORD
    buffer = ctypes.create_unicode_buffer(32768)
    length = api(path, buffer, len(buffer))
    if not length or length >= len(buffer):
        raise OSError('could not expand artifact path aliases')
    value = buffer.value
    if value.startswith('\\\\?\\UNC\\'):
        value = '\\\\' + value[8:]
    elif value.startswith('\\\\?\\'):
        value = value[4:]
    return os.path.normcase(os.path.normpath(value))


def open_source_windows(root, relative):
    """Open and verify a Windows source handle, refusing all reparse points."""
    if os.name != 'nt':
        raise OSError('Windows source open requested on a different platform')
    import ctypes
    import msvcrt
    from ctypes import wintypes

    parts = _components(relative)
    # DOS devices and alternate streams have semantics other than normal files.
    for part in parts:
        if part.endswith(('.', ' ')) or part.split('.')[0].upper() in (
                {'CON', 'PRN', 'AUX', 'NUL'} | {f'COM{i}' for i in range(1, 10)} |
                {f'LPT{i}' for i in range(1, 10)}):
            raise ValueError('invalid Windows artifact component')
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    create = api.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                       wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = api.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    attrs = api.GetFileInformationByHandleEx
    attrs.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD]
    attrs.restype = wintypes.BOOL
    final = api.GetFinalPathNameByHandleW
    final.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
    final.restype = wintypes.DWORD
    invalid = ctypes.c_void_p(-1).value
    path = os.path.abspath(root)
    for i, part in enumerate([None, *parts]):
        if part is not None:
            path = os.path.join(path, part)
        # OPEN_REPARSE_POINT means the final component is opened, not resolved.
        handle = create(path, 0x80000000, 7, None, 3, 0x00200000 | 0x02000000, None)
        if handle == invalid:
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            data = (wintypes.DWORD * 2)()
            if not attrs(handle, 9, ctypes.byref(data), ctypes.sizeof(data)):
                raise ctypes.WinError(ctypes.get_last_error())
            if data[0] & 0x400:
                raise OSError('artifact path contains a reparse point')
            is_dir = bool(data[0] & 0x10)
            if is_dir != (i < len(parts)):
                raise OSError('artifact path has incorrect file type')
            buffer = ctypes.create_unicode_buffer(32768)
            length = final(handle, buffer, len(buffer), 0)
            if not length or length >= len(buffer):
                raise OSError('could not resolve artifact handle')
            resolved = buffer.value
            if resolved.startswith('\\\\?\\UNC\\'):
                resolved = '\\\\' + resolved[8:]
            elif resolved.startswith('\\\\?\\'):
                resolved = resolved[4:]
            if os.path.normcase(os.path.normpath(resolved)) != _windows_long_path(path):
                raise OSError('artifact handle escaped or changed its source path')
            if not is_dir:
                fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
                handle = None  # open_osfhandle owns it
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode):
                    os.close(fd)
                    raise OSError('artifact is not a regular file')
                return os.fdopen(fd, 'rb'), info
        finally:
            if handle is not None:
                close(handle)
    raise OSError('artifact path is empty')


def source_identity(info):
    """Fields that detect ordinary replacement, mutation, and growth."""
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def _nt_open_relative(parent, name, access, disposition, options, share=3):
    """Open one Windows component relative to a captured directory handle."""
    if os.name != 'nt' or _components(name) != [name]:
        raise OSError('invalid native relative open')
    import ctypes
    from ctypes import wintypes

    class UnicodeString(ctypes.Structure):
        _fields_ = [('Length', wintypes.USHORT), ('MaximumLength', wintypes.USHORT),
                    ('Buffer', wintypes.LPWSTR)]

    class ObjectAttributes(ctypes.Structure):
        _fields_ = [('Length', wintypes.ULONG), ('RootDirectory', wintypes.HANDLE),
                    ('ObjectName', ctypes.POINTER(UnicodeString)), ('Attributes', wintypes.ULONG),
                    ('SecurityDescriptor', wintypes.LPVOID), ('SecurityQualityOfService', wintypes.LPVOID)]

    class IoStatus(ctypes.Structure):
        _fields_ = [('status', ctypes.c_void_p), ('information', ctypes.c_size_t)]

    ntdll = ctypes.WinDLL('ntdll')
    ntcreate = ntdll.NtCreateFile
    ntcreate.argtypes = [ctypes.POINTER(wintypes.HANDLE), wintypes.ULONG, ctypes.POINTER(ObjectAttributes),
                         ctypes.POINTER(IoStatus), ctypes.c_void_p, wintypes.ULONG, wintypes.ULONG,
                         wintypes.ULONG, wintypes.ULONG, ctypes.c_void_p, wintypes.ULONG]
    ntcreate.restype = ctypes.c_long
    nt_to_dos = ntdll.RtlNtStatusToDosError
    nt_to_dos.argtypes = [ctypes.c_long]
    nt_to_dos.restype = wintypes.ULONG
    buffer = ctypes.create_unicode_buffer(name)
    encoded_length = len(name.encode('utf-16le'))
    if encoded_length > 65532:
        raise OSError('native artifact component too long')
    string = UnicodeString(encoded_length, encoded_length + 2, ctypes.cast(buffer, wintypes.LPWSTR))
    attrs = ObjectAttributes(ctypes.sizeof(ObjectAttributes), parent, ctypes.pointer(string),
                             0x40 | 0x1000, None, None)  # CASE_INSENSITIVE | DONT_REPARSE
    status, result = IoStatus(), wintypes.HANDLE()
    code = ntcreate(ctypes.byref(result), access, ctypes.byref(attrs), ctypes.byref(status),
                    None, 0, share, disposition, options | 0x00200000, None, 0)
    if code < 0:
        raise ctypes.WinError(nt_to_dos(code))
    return result.value


def windows_create_temp(parent, name):
    """Create a new temp file through the pinned parent; return a Python file."""
    import msvcrt
    # GENERIC_WRITE | DELETE | SYNCHRONIZE, FILE_CREATE, NON_DIRECTORY | sync.
    handle = _nt_open_relative(parent, name, 0x40110000, 2, 0x60, share=0)
    try:
        fd = msvcrt.open_osfhandle(handle, os.O_BINARY | os.O_WRONLY)
    except BaseException:
        import ctypes
        from ctypes import wintypes
        close = ctypes.WinDLL('kernel32').CloseHandle
        close.argtypes = [wintypes.HANDLE]
        close(handle)
        raise
    return os.fdopen(fd, 'wb')


def windows_publish(file, parent, leaf):
    """Atomically rename an open temp to a new leaf relative to the handle."""
    if _components(leaf) != [leaf]:
        raise ValueError('invalid destination leaf')
    import ctypes
    import msvcrt
    from ctypes import wintypes

    name_bytes = len(leaf.encode('utf-16le'))
    class RenameInfo(ctypes.Structure):
        _fields_ = [('ReplaceIfExists', wintypes.BOOL), ('RootDirectory', wintypes.HANDLE),
                    ('FileNameLength', wintypes.DWORD), ('FileName', ctypes.c_wchar * (name_bytes // 2 + 1))]

    info = RenameInfo(False, parent, name_bytes, leaf)
    class IoStatus(ctypes.Structure):
        _fields_ = [('status', ctypes.c_void_p), ('information', ctypes.c_size_t)]
    api = ctypes.WinDLL('ntdll')
    call = api.NtSetInformationFile
    call.argtypes = [wintypes.HANDLE, ctypes.POINTER(IoStatus), wintypes.LPVOID,
                     wintypes.ULONG, ctypes.c_int]
    call.restype = ctypes.c_long
    to_dos = api.RtlNtStatusToDosError
    to_dos.argtypes = [ctypes.c_long]
    to_dos.restype = wintypes.ULONG
    file.flush()
    os.fsync(file.fileno())
    status = IoStatus()
    code = call(msvcrt.get_osfhandle(file.fileno()), ctypes.byref(status), ctypes.byref(info),
                RenameInfo.FileName.offset + info.FileNameLength, 10)
    if code < 0:
        raise ctypes.WinError(to_dos(code))


def windows_discard(file):
    """Mark an open temporary file for deletion on close."""
    import ctypes
    import msvcrt
    from ctypes import wintypes
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    call = api.SetFileInformationByHandle
    call.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD]
    call.restype = wintypes.BOOL
    delete = wintypes.BOOL(True)
    if not call(msvcrt.get_osfhandle(file.fileno()), 4, ctypes.byref(delete), ctypes.sizeof(delete)):
        raise ctypes.WinError(ctypes.get_last_error())


@contextlib.contextmanager
def destination_parent(root, relative):
    """Pin every destination directory under root until publication completes.

    On Windows directory handles deny delete sharing and all child operations
    resolve relative to the held handle with OBJ_DONT_REPARSE.
    On POSIX all traversal and publication is relative to pinned directory fds.
    """
    parts = _components(relative)
    handles = []
    if os.name != 'nt':
        if not hasattr(os, 'O_NOFOLLOW') or not hasattr(os, 'O_DIRECTORY'):
            raise OSError('safe destination traversal unavailable')
        try:
            fd = _open_directory_posix(root, own=True)      # the queue's state directory, not a job's
            handles.append(fd)
            for part in parts[:-1]:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=fd)
                except FileExistsError:
                    pass
                fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                handles.append(fd)
            yield os.path.join(root, *parts[:-1]), fd, parts[-1]
        finally:
            for handle in reversed(handles):
                os.close(handle)
        return

    import ctypes
    from ctypes import wintypes
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    create = api.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                       wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = api.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    attrs = api.GetFileInformationByHandleEx
    attrs.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD]
    attrs.restype = wintypes.BOOL
    final = api.GetFinalPathNameByHandleW
    final.argtypes = [wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD]
    final.restype = wintypes.DWORD
    path = os.path.abspath(root)
    try:
        for part in [None, *parts[:-1]]:
            if part is not None:
                path = os.path.join(path, part)
                # FILE_OPEN_IF | FILE_DIRECTORY_FILE | synchronous I/O.
                handle = _nt_open_relative(handles[-1], part, 0x80100000, 3, 0x21)
            else:
                handle = create(path, 0x80000000, 3, None, 3, 0x02200000, None)
                if handle == ctypes.c_void_p(-1).value:
                    raise ctypes.WinError(ctypes.get_last_error())
            handles.append(handle)
            data = (wintypes.DWORD * 2)()
            if not attrs(handle, 9, ctypes.byref(data), ctypes.sizeof(data)):
                raise ctypes.WinError(ctypes.get_last_error())
            if data[0] & 0x400 or not data[0] & 0x10:
                raise OSError('unsafe destination directory')
            buffer = ctypes.create_unicode_buffer(32768)
            length = final(handle, buffer, len(buffer), 0)
            if not length or length >= len(buffer):
                raise OSError('could not resolve destination directory')
            resolved = buffer.value
            if resolved.startswith('\\\\?\\UNC\\'):
                resolved = '\\\\' + resolved[8:]
            elif resolved.startswith('\\\\?\\'):
                resolved = resolved[4:]
            if os.path.normcase(os.path.normpath(resolved)) != _windows_long_path(path):
                raise OSError('destination directory escaped or changed')
        yield path, handles[-1], parts[-1]
    finally:
        for handle in reversed(handles):
            close(handle)


def hash_stream(stream, limit):
    """Hash a stream incrementally, refusing actual bytes above the cap."""
    digest = hashlib.sha256()
    size = 0
    while True:
        data = stream.read(min(CHUNK, limit - size + 1))
        if not data:
            return size, digest.hexdigest()
        size += len(data)
        if size > limit:
            raise ValueError('artifact exceeds limit')
        digest.update(data)


class CaptureSink:
    """Publish verified frames with a durable caller-owned pending record.

    ``record(entry)`` must durably persist the entry before returning. ``done``
    must durably merge the capture into job metadata and clear that record.
    The caller serializes capture/recovery for a job. If publication succeeds
    but ``done`` fails, the record and final file remain for reconciliation.
    """
    def __init__(self, state_root, job_id, record, done, clear, reserve=DISK_RESERVE):
        _components(job_id)
        if '/' in job_id:
            raise ValueError('invalid job id')
        self.root, self.job_id, self.record, self.done, self.clear = state_root, job_id, record, done, clear
        self.reserve = reserve
        self.file = self.guard = self.entry = None
        self.published = self.recorded = False
        self.next_space_check = 0

    def _space(self, additional):
        if shutil.disk_usage(self.root).free < self.reserve + additional:
            raise OSError('insufficient local disk space for artifact reserve')

    def begin(self, index, relative, size):
        _components(relative)
        self._space(size)
        self.guard = destination_parent(self.root, f'out/{self.job_id}/{relative}')
        try:
            parent, directory, leaf = self.guard.__enter__()
        except BaseException:
            self.guard = None
            raise
        self.parent, self.directory, self.leaf = parent, directory, leaf
        self.entry = {'path': relative, 'temp': '.cq-artifact-' + secrets.token_hex(16),
                      'size_bytes': size, 'phase': 'staging'}
        self.published = self.recorded = False
        self.record(dict(self.entry))
        self.recorded = True
        if os.name == 'nt':
            self.file = windows_create_temp(directory, self.entry['temp'])
        else:
            fd = os.open(self.entry['temp'], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=directory)
            self.file = os.fdopen(fd, 'wb')
        self.next_space_check = 16 * MIB

    def write(self, data):
        self.file.write(data)
        self.next_space_check -= len(data)
        if self.next_space_check <= 0:
            self._space(0)
            self.next_space_check = 16 * MIB

    def commit(self, index, relative, size, digest):
        self.file.flush()
        os.fsync(self.file.fileno())
        self._space(0)
        self.entry.update(phase='verified', sha256=digest)
        self.record(dict(self.entry))
        if os.name == 'nt':
            windows_publish(self.file, self.directory, self.leaf)
            self.published = True
            os.fsync(self.file.fileno())
        else:
            os.link(self.entry['temp'], self.leaf, src_dir_fd=self.directory, dst_dir_fd=self.directory,
                    follow_symlinks=False)
            self.published = True
            os.unlink(self.entry['temp'], dir_fd=self.directory)
            os.fsync(self.directory)
        self.file.close()
        self.file = None
        self.done(dict(self.entry))
        self.entry = None
        self._close_guard()

    def _close_guard(self):
        if self.guard is not None:
            self.guard.__exit__(None, None, None)
            self.guard = None

    def abort(self):
        try:
            if self.file is not None:
                if not self.published and os.name == 'nt':
                    windows_discard(self.file)
                self.file.close()
                self.file = None
            if self.entry is not None and not self.published and os.name != 'nt':
                try:
                    os.unlink(self.entry['temp'], dir_fd=self.directory)
                except FileNotFoundError:
                    pass
            if self.entry is not None and not self.published and self.recorded:
                self.clear(dict(self.entry))
                self.entry = None
        finally:
            self._close_guard()


def recover_pending(state_root, job_id, pending, done, clear):
    """Reconcile an interrupted publication without replaying the job.

    A verified final file is adopted only when its bytes match the durable
    record. Staging temps are removed through the pinned parent. Returns False
    on conflicting final content, leaving the record and files for inspection.
    """
    _components(job_id)
    if '/' in job_id or not isinstance(pending, dict):
        raise ValueError('invalid artifact recovery record')
    relative, temp = pending.get('path'), pending.get('temp')
    _components(relative)
    if not isinstance(temp, str) or not re.fullmatch(r'\.cq-artifact-[0-9a-f]{32}', temp):
        raise ValueError('invalid artifact recovery temp')
    phase = pending.get('phase')
    size = pending.get('size_bytes')
    if phase not in ('staging', 'verified') or type(size) is not int or not 0 <= size <= MAX_FILE:
        raise ValueError('invalid artifact recovery metadata')
    digest = pending.get('sha256')
    if phase == 'verified' and (not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest)):
        raise ValueError('invalid artifact recovery digest')

    def open_leaf(directory, leaf, delete=False):
        if os.name == 'nt':
            import msvcrt
            access = 0x80110000 if delete else 0x80100000
            handle = _nt_open_relative(directory, leaf, access, 1, 0x60, share=0 if delete else 3)
            try:
                import ctypes
                from ctypes import wintypes
                api = ctypes.WinDLL('kernel32', use_last_error=True)
                query = api.GetFileInformationByHandleEx
                query.argtypes = [wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID, wintypes.DWORD]
                query.restype = wintypes.BOOL
                data = (wintypes.DWORD * 2)()
                if not query(handle, 9, ctypes.byref(data), ctypes.sizeof(data)):
                    raise ctypes.WinError(ctypes.get_last_error())
                if data[0] & (0x400 | 0x10):
                    raise OSError('invalid recovery file type')
                fd = msvcrt.open_osfhandle(handle, os.O_BINARY | os.O_RDONLY)
            except BaseException:
                close = ctypes.WinDLL('kernel32').CloseHandle
                close.argtypes = [wintypes.HANDLE]
                close(handle)
                raise
        else:
            fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            os.close(fd)
            raise OSError('invalid recovery file')
        return os.fdopen(fd, 'rb')

    def missing(exc):
        return isinstance(exc, FileNotFoundError) or getattr(exc, 'winerror', None) in (2, 3)

    with destination_parent(state_root, f'out/{job_id}/{relative}') as (_, directory, leaf):
        adopted = False
        if phase == 'verified':
            try:
                with open_leaf(directory, leaf) as final:
                    actual_size, actual_digest = hash_stream(final, size)
                if (actual_size, actual_digest) != (size, digest):
                    return False
                adopted = True
            except OSError as exc:
                if not missing(exc):
                    raise
            except ValueError:
                return False
        try:
            with open_leaf(directory, temp, delete=True) as staged:
                if os.name == 'nt':
                    windows_discard(staged)
                else:
                    os.unlink(temp, dir_fd=directory)
        except OSError as exc:
            if not missing(exc):
                raise
        if adopted:
            done(dict(pending))
        else:
            clear(dict(pending))
        return True


def capture_local(root, wanted, maximum, remaining, sink):
    """Copy local sources in bounded chunks through the same journaled sink."""
    opener = open_source_windows if os.name == 'nt' else open_source_posix
    missing = []
    for index, relative in wanted:
        sink.published = False
        try:
            source, before = opener(root, relative)
            with source:
                size = before.st_size
                if size > maximum or size > remaining:
                    raise ValueError('artifact exceeds quota')
                sink.begin(index, relative, size)
                digest = hashlib.sha256()
                left = size
                while left:
                    data = source.read(min(CHUNK, left))
                    if not data:
                        raise OSError('artifact shrank during capture')
                    sink.write(data)
                    digest.update(data)
                    left -= len(data)
                if source_identity(before) != source_identity(os.fstat(source.fileno())) or source.read(1):
                    raise OSError('artifact changed during capture')
                sink.commit(index, relative, size, digest.hexdigest())
                remaining -= size
        except (OSError, ValueError):
            sink.abort()
            if sink.published:
                raise  # durable pending record must be reconciled first
            missing.append(relative)
    return missing


def _line(writer, value):
    data = json.dumps(value, separators=(',', ':'), ensure_ascii=True).encode('ascii') + b'\n'
    if len(data) > MAX_HEADER:
        raise ValueError('artifact frame header too long')
    writer.write(data)


def _header(reader, limit=MAX_HEADER):
    line = reader.readline(limit + 1)
    if not line or len(line) > limit or not line.endswith(b'\n'):
        raise ValueError('truncated or oversized artifact frame')
    try:
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError('duplicate artifact frame key')
                result[key] = value
            return result
        result = json.loads(line, object_pairs_hook=unique)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError('invalid artifact frame') from exc
    if not isinstance(result, dict):
        raise ValueError('invalid artifact frame')
    return result


def send(root, wanted, maximum, remaining, writer, open_source=None):
    """Stream a framed, bounded snapshot. Never follow source links.

    The worker must be confirmed empty before calling this function. A changing
    file is marked bad in its trailer and the receiver must discard its bytes.
    """
    if open_source is None:
        open_source = open_source_windows if os.name == 'nt' else open_source_posix
    for index, relative in wanted:
        try:
            source, before = open_source(root, relative)
        except (OSError, ValueError):
            _line(writer, {'index': index, 'missing': True})
            continue
        with source:
            size = before.st_size
            if size > maximum or size > remaining:
                _line(writer, {'index': index, 'missing': True})
                continue
            _line(writer, {'index': index, 'size': size})
            digest = hashlib.sha256()
            left = size
            while left:
                data = source.read(min(CHUNK, left))
                if not data:
                    raise OSError('artifact shrank during capture')
                writer.write(data)
                digest.update(data)
                left -= len(data)
            stable = source_identity(before) == source_identity(os.fstat(source.fileno())) and not source.read(1)
            _line(writer, {'sha256': digest.hexdigest(), 'stable': stable})
            if stable:
                remaining -= size
    _line(writer, {'end': len(wanted)})
    writer.flush()


def receive(reader, wanted, maximum, remaining, sink):
    """Receive frames into a staging sink and verify before publication.

    sink.begin(index, path, size), sink.write(bytes), sink.commit(index, path,
    size, digest), sink.abort() implement durable local publication and journal.
    On any framing or transfer failure the current staged file is aborted.
    """
    missing = []
    try:
        for index, relative in wanted:
            header = _header(reader)
            if type(header.get('index')) is not int or header['index'] != index:
                raise ValueError('unexpected artifact index')
            if header == {'index': index, 'missing': True}:
                missing.append(relative)
                continue
            if set(header) != {'index', 'size'} or type(header['size']) is not int:
                raise ValueError('invalid artifact size frame')
            size = header['size']
            if size < 0 or size > maximum or size > remaining:
                raise ValueError('artifact exceeds quota')
            sink.begin(index, relative, size)
            digest, left = hashlib.sha256(), size
            while left:
                data = reader.read(min(CHUNK, left))
                if not data:
                    raise ValueError('truncated artifact body')
                sink.write(data)
                digest.update(data)
                left -= len(data)
            trailer = _header(reader)
            actual = digest.hexdigest()
            if (set(trailer) != {'sha256', 'stable'} or trailer.get('stable') is not True
                    or trailer.get('sha256') != actual):
                sink.abort()
                missing.append(relative)
                continue
            sink.commit(index, relative, size, actual)
            remaining -= size
        if _header(reader) != {'end': len(wanted)}:
            raise ValueError('invalid artifact end frame')
    except BaseException:
        sink.abort()
        raise
    return missing
