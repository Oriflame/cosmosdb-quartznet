using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Quartz.Spi.CosmosDbJobStore.Entities;
using Quartz.Spi.CosmosDbJobStore.Repositories;

namespace Quartz.Spi.CosmosDbJobStore
{
    /// <inheritdoc />
    /// <summary>
    /// Implements a simple distributed lock on top of CosmosDB. It is not a reentrant lock so you can't 
    /// acquire the lock more than once in the same thread of execution.
    /// </summary>
    internal class LockManager : IDisposable
    {
        private static readonly TimeSpan SleepThreshold = TimeSpan.FromSeconds(1);
        private const int LockTtl = 5 * 60; // 5 minutes (after this, Lock is released automatically)
        
        private readonly ConcurrentDictionary<LockType, DisposableLock> _pendingLocks = new ConcurrentDictionary<LockType, DisposableLock>();
        private readonly LockRepository _lockRepository;
        private readonly string _instanceName;

        private bool _disposed;
        
        
        public LockManager(LockRepository lockRepository, string instanceName)
        {
            _lockRepository = lockRepository;
            _instanceName = instanceName;
        }

        
        public void Dispose()
        {
            EnsureObjectNotDisposed();

            _disposed = true;
            var locks = _pendingLocks.ToArray();
            foreach (var keyValuePair in locks)
            {
                keyValuePair.Value.Dispose();
            }
        }

        public async Task<IDisposable> AcquireLock(LockType lockType, string instanceId)
        {
            while (true)
            {
                EnsureObjectNotDisposed();
                
                var lck = new PersistentLock(_instanceName, lockType, DateTimeOffset.Now, instanceId, LockTtl);
                
                if (await _lockRepository.TrySave(lck))
                {
                    var disposableLock = new DisposableLock(this, lck);
                    
                    if (!_pendingLocks.TryAdd(lockType, disposableLock))
                    {
                        throw new InvalidOperationException($"Unable to add lock instance for lock {lockType} on {instanceId}");
                    }

                    return disposableLock;
                }
                
                Thread.Sleep(SleepThreshold);
            }
        }

        private void EnsureObjectNotDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(LockManager));
            }
        }

        private class DisposableLock : IDisposable
        {
            private static readonly ILog _logger = LogManager.GetLogger<LockManager>();
            
            private readonly LockManager _lockManager;
            private readonly PersistentLock _lck;

            private bool _disposed;

            
            public DisposableLock(LockManager lockManager, PersistentLock lck)
            {
                _lockManager = lockManager;
                _lck = lck;
            }


            public void Dispose()
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(DisposableLock), $"This lock {_lck.Id} has already been disposed");
                }

                if (!_lockManager._lockRepository.TryDelete(_lck.Id).GetAwaiter().GetResult())
                {
                    _logger.Warn($"Unable to delete pending lock {_lck.Id} from storage. It may have expired.");
                }
                
                if (!_lockManager._pendingLocks.TryRemove(_lck.LockType, out _))
                {
                    _logger.Warn($"Unable to remove pending lock {_lck.Id} from in-memory collection.");
                }

                _disposed = true;
            }
        }
    }
}