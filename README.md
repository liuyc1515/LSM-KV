# Project LSM-KV: KVStore using Log-structured Merge Tree

# Build Correctness Test

    cmake -B build -DLSMKV_CORRECTNESS_TEST=True
    cmake --build build

# Build Persistence Test

    cmake -B build -DLSMKV_CORRECTNESS_TEST=False
    cmake --build build
