These are test-only certificates and a test-only leaf private key.

`chain.pem` contains the localhost leaf certificate followed by its intermediate.
The leaf supports server and client authentication. `root.pem` is the trust anchor;
the root and intermediate private keys are not needed by these tests.
