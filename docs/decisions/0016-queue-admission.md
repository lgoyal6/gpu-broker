# 0016. Serialize concurrent queue admission

Status: accepted, 2026-10-15

Submit takes a dedicated PostgreSQL transaction advisory lock before reading
tenant and global queue counts. It retains that lock through insertion and
commit. Different request keys and tenants share this admission boundary;
idempotency key ownership alone cannot protect a global queue limit.

The global count makes admission a serialized operation. This avoids a new
counter table and schema migration. Quota and queue refusals still write no
job row and can be retried. Cancellation can reopen an admission slot.
Recovery requeues existing jobs separately; the limits govern new admissions
and do not drop recovery work to enforce an absolute queue-size bound.

Eight simultaneous submissions test tenant and global limits independently,
including different tenants, serial refusal, and cancellation reopening one
slot. Independent before-fix PostgreSQL barriers demonstrated two admissions
against a limit of one. Capacity reservation and budget settlement retain
their separate worker and project locks.
