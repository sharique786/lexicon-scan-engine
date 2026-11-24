#!/usr/bin/env python3
"""
Removes a stack-buffer overflow from Vectorscan 5.4.11 as shipped in
com.gliwka.hyperscan:native:5.4.11-2.0.2:linux-x86_64 (used by hyperscan-java 5.4.11-3.1.0).

The bug: rvermicelliDoubleExecReal (src/nfa/vermicelli_simd.cpp) still contains leftover debug code,
compiled into the release build for every CPU variant of the fat runtime:

    char s[255];
    snprintf(s, buf_end - buf + 1, "%s", buf);   // size is the SCANNED REGION length, not sizeof(s)

Whenever Hyperscan runs that reverse double-byte search over more than ~255 bytes of text that contain no
NUL byte (i.e. almost any real message), snprintf writes text past `s` over the saved registers and the
return address. The function's `ret` then jumps to "address" = 8 bytes of message text and the JVM dies:

    SIGSEGV ... C  [libhs_runtime.so+0x4a6c91]      (AVX-512 variant; the offset is that `ret`)

The snprintf result is never used (the following DEBUG_PRINTF is compiled out, and the next instruction
overwrites rax at every site), so each `call snprintf@plt` is replaced by a 5-byte NOP. Nothing else changes.

Usage:  python3 patch_vectorscan.py <dir with original libhs.so and libhs_runtime.so> <output dir>
The input hashes are checked, and each site's bytes are checked to be `call snprintf@plt` before patching.
"""
import hashlib
import os
import struct
import sys

NOP5 = bytes([0x0F, 0x1F, 0x44, 0x00, 0x00])

# file offset == virtual address in .text for both libraries (first R E LOAD segment maps offset 1:1)
LIBS = {
    "libhs_runtime.so": {
        "sha256_in": "29c340449f77fef4359e6a5b121f81fcaa3dd81a3e7bb4ee14ebb5908b24c5a7",
        "snprintf_plt": 0x6040,
        # rvermicelliDoubleExecReal: core2, corei7, avx2, avx512 variants
        "sites": [0x118349, 0x225619, 0x33E8DE, 0x4A6A4C],
    },
    "libhs.so": {
        "sha256_in": "4f7763cdc55c884da1aa4d822ddf3eab4e17c0e990bd760f9a8268b5165f80f2",
        "snprintf_plt": 0x2C2D0,
        "sites": [0x47E389, 0x58B659, 0x6A491E, 0x80CA8C],
    },
}


def patch(name, spec, src_dir, out_dir):
    data = bytearray(open(os.path.join(src_dir, name), "rb").read())
    digest = hashlib.sha256(data).hexdigest()
    if digest != spec["sha256_in"]:
        sys.exit(f"{name}: unexpected input sha256 {digest} - not the 5.4.11-2.0.2 linux-x86_64 build")
    for site in spec["sites"]:
        if data[site] != 0xE8:
            sys.exit(f"{name}+{site:#x}: expected a call instruction, found {data[site]:#04x}")
        rel = struct.unpack_from("<i", data, site + 1)[0]
        target = site + 5 + rel
        if target != spec["snprintf_plt"]:
            sys.exit(f"{name}+{site:#x}: call target {target:#x} is not snprintf@plt {spec['snprintf_plt']:#x}")
        data[site:site + 5] = NOP5
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, name), "wb") as f:
        f.write(data)
    print(f"{name}: patched {len(spec['sites'])} sites, sha256 {hashlib.sha256(data).hexdigest()}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    for lib_name, lib_spec in LIBS.items():
        patch(lib_name, lib_spec, sys.argv[1], sys.argv[2])
