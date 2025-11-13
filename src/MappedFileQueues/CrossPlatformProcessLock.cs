using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace MappedFileQueues;

/// <summary>
/// A cross-platform process lock implementation. Only Windows and Linux are supported.
/// On Windows, a named mutex is used. On Linux, a file lock is used. On other platforms, a no-op lock is used.
/// </summary>
internal sealed class CrossPlatformProcessLock : IDisposable
{
    private readonly IProcessLock _lock;

    /// <summary>
    /// Initializes a new instance of the <see cref="CrossPlatformProcessLock"/> class.
    /// </summary>
    /// <param name="name">The name of the process lock.</param>
    /// <param name="storePath">The store path of the <see cref="MappedFileQueue{T}"/>.</param>
    public CrossPlatformProcessLock(string name, string storePath)
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            var hashBytes = MD5.HashData(System.Text.Encoding.UTF8.GetBytes(storePath));
            var hashString = Convert.ToHexString(hashBytes);
            name = $"{name}_{hashString}";
            _lock = new MutexProcessLock(name);
        }
        else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
        {
            var lockFilePath = Path.Combine(storePath, $"{name}.lock");
            _lock = new FileProcessLock(lockFilePath);
        }
        else
        {
            _lock = new EmptyProcessLock();
        }
    }

    public void Acquire() => _lock.Acquire();
    public void Release() => _lock.Release();

    public void Dispose() => _lock.Dispose();

    #region Process Lock Interface

    private interface IProcessLock : IDisposable
    {
        void Acquire();
        void Release();
    }

    #endregion

    #region Windows Named Mutex Lock

    private class MutexProcessLock(string name) : IProcessLock
    {
        private Mutex? _mutex;
        // Global\ Make the mutex available across sessions
        private readonly string _name = $@"Global\{name}";

        public void Acquire()
        {
            _mutex = new Mutex(false, _name);
            _mutex.WaitOne();
        }

        public void Release()
        {
            _mutex?.ReleaseMutex();
            _mutex?.Dispose();
        }

        public void Dispose() => Release();
    }

    #endregion

    #region Linux File Lock

    private class FileProcessLock(string lockFilePath) : IProcessLock
    {
        private FileStream? _lockFileStream;

        public void Acquire()
        {
            while (true)
            {
                try
                {
                    _lockFileStream = new FileStream(lockFilePath, FileMode.OpenOrCreate,
                        FileAccess.ReadWrite, FileShare.ReadWrite);
                    _lockFileStream.Lock(0, 0);
                    return;
                }
                catch (IOException)
                {
                    // Lock is held by another process, wait and retry
                    Thread.Sleep(200);
                }
            }
        }

        public void Release()
        {
            _lockFileStream?.Unlock(0, 0);
            _lockFileStream?.Dispose();
        }

        public void Dispose() => Release();
    }

    #endregion

    #region Empty Lock (for unsupported platforms)

    private class EmptyProcessLock : IProcessLock
    {
        public void Acquire()
        {
        }

        public void Release()
        {
        }

        public void Dispose()
        {
        }
    }

    #endregion
}
