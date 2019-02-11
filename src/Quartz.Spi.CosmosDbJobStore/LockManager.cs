using System;
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
        
        private readonly LockRepository _lockRepository;
        private readonly string _instanceName;
        private readonly string _instanceId;
        
        private static readonly ILog _logger = LogManager.GetLogger<LockManager>();

        private bool _disposed;
        
        
        public LockManager(LockRepository lockRepository, string instanceName, string instanceId)
        {
            _lockRepository = lockRepository;
            _instanceName = instanceName;
            _instanceId = instanceId;
        }

        
        public void Dispose()
        {
            EnsureObjectNotDisposed();

            _disposed = true;

            var locks = _lockRepository.GetAllByInstanceId(_instanceId).GetAwaiter().GetResult();
            
            foreach (var lck in locks)
            {
                if (!_lockRepository.TryDelete(lck.Id).GetAwaiter().GetResult())
                {
                    _logger.Warn($"Unable to delete pending lock {lck.Id} from storage.");
                }
            }
        }

        public async Task<IDisposable> AcquireLock(LockType lockType)
        {
            while (true)
            {
                EnsureObjectNotDisposed();
                
                var lck = new PersistentLock(_instanceName, lockType, DateTimeOffset.UtcNow, _instanceId, LockTtl);
                
                if (await _lockRepository.TrySave(lck))
                {
                    var disposableLock = new DisposableLock(this, lck);
                    
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
                
                _disposed = true;
            }
        }
    }
}