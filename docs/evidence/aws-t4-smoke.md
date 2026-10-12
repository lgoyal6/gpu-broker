# AWS T4 smoke evidence

Recorded 2026-10-12 UTC in `us-east-1` with the `g4dn.xlarge` Deep Learning
Base AMI `ami-0a24e16849196176a`.

Command:

```text
gpu submit --gpu t4 --hours 0.01 -- nvidia-smi --query-gpu=name,memory.total --format=csv,noheader
gpu run --interval 10
```

The broker launched one EC2 instance, delivered the command through SSM, and
completed the job with exit code 0. SSM returned:

```text
Tesla T4, 15360 MiB
```

The job cost `$0.01` and the instance was terminated after completion. This
proves the configured EC2, IAM, AMI, SSM, and GPU execution path for one
on-demand smoke job. It does not prove multi-node capacity, long-running
training, spot interruption recovery on AWS, production adoption, or a
hardware benchmark.
