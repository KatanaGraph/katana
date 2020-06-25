AvA-based GPU service
=====================

Build
=====

AvA and its code generation depend on several Ubuntu tools and Python3 packages:

```Shell
sudo apt install libclang-7-dev clang-7 indent libglib2.0-dev
python3 -m pip install toposort blessings astor
```

[CUDA 10.1][cuda_run] is tested. CUDA >=9.0 and <=10.0 are also supported in theory.
The default Linux kernel is 5.x on AWS EC2, so the NVIDIA driver packed in the CUDA 10.1
installer cannot be installed.
The NVIDIA driver 440.33.01 coming with CUDA 10.2 installer is tested.

Most dependencies will be downloaded or prompted during the build time.
To build AvA's API remoting stubs, specify `GALOIS_CUDA_CAPABILITY` and enable
`GALOIS_ENABLE_AVA` when configuring Katana. For example

```Shell
cmake -DCMAKE_TOOLCHAIN_FILE=conan_paths.cmake \
      -DCMAKE_C_COMPILER=gcc-7 -DCMAKE_CXX_COMPILER=g++-7 \
      -DCMAKE_CUDA_COMPILER="/usr/local/cuda-10.1/bin/nvcc" \
      -DGALOIS_CUDA_CAPABILITY=7.5 -DGALOIS_ENABLE_AVA=ON
      ../katana
```

AWS G3 instance has a NVIDIA M60 GPU which has CUDA capability 5.2;
G4 instance has a T4 GPU which has CUDA capability 7.5.

Then running `make` will build everything.

[cuda_run]: http://developer.download.nvidia.com/compute/cuda/10.1/Prod/local_installers/cuda_10.1.243_418.87.00_linux.run

Configuration
=============

AvA spawn daemon requires a GPU ID list file as input.
`${KATANA_SOURCE_DIR}/libava/manager/gpu_example.conf` is an example.
The GPU UUID can be parsed from `nvidia -L` command.

The guestlib requires a configuration file at `/etc/ava/guest.conf`. The details of the
entries can be found at [AvA's official repository](https://github.com/yuhc/ava/tree/master/config).

```json
channel = "TCP";
manager_address = "0.0.0.0:3334";
gpu_memory = [1024L];
```

Use AvA
=======

Assume we are at `${KATANA_BUILD_DIR}`.
Start the AvA manager by the following command (which runs the manager service
at port `3334` and sets the API server pool size to `2`).
Note that the port `3333` is reserved by the manager in the current version, used
for guestlib's connection.

```Shell
./libava/manager/manager -m 3334 -n 2
```

Open another terminal, start the AvA spawn daemon on the local machine (as
this example) or a remote GPU server:

```Shell
./libava/manager/spawn_daemon -f gpu.conf -w libava/generated/worker -m 0.0.0.0:3334
```

The daemon's port can be set with `-p 3335`, and the API server's base port can be set
with `-b 4000`.
The daemon will display and register the GPU information in the manager.
The manager will also request it to spawn an API server pool on every GPU.

Then the application can be started by loading AvA's generated CUDA library.
In the build directory (`${KATANA_BUILD_DIR}`):

```Shell
LD_LIBRARY_PATH=libava/generated:libava/ava-bin/third_party/grpc/lib AVA_MANAGER_ADDR=0.0.0.0:3334 \
./lonestar/analytics/pagerank/pagerank-gpu inputs/stanford/communities/DBLP/com-dblp.wgt32.sym.gr.triangles
```

`AVA_WORKER_ADDR` is the IP address of the GPU node running the AvA manager.
