--[[

Find any Jobs that have been abandoned.
Clean extraneous worker entries.

for each worker in <ns>:WORKERS
    if the worker is no longer active
        for each job in <ns>:WORKERS:<worker_name>
            if job key exists
                fail job appropriately (schedule or fail)
        delete key <ns>:WORKERS:<worker_name>
        remove <worker_name> from <ns>:WORKERS
--]]
