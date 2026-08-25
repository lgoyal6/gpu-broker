-- Cancelling from the browser, when the browser cannot reach the machines.
--
-- The web app deliberately holds no AWS credentials and no SSH key, so it
-- cannot terminate anything. A cancel from a web page is therefore a *request*
-- that the scheduler daemon acts on: it flips this flag, and the next tick
-- stops the machine and moves the job.
--
-- A queued job is different -- there is no machine involved -- so the web app
-- cancels those outright.

ALTER TABLE jobs ADD COLUMN cancel_requested TEXT;
