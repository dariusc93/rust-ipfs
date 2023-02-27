# 0.3.0-alpha.7 [unrelease]
- refactor/transport: Simplify TransportConfig and added additional options [PR 32]
- chore: Update to libp2p 0.51.0 [PR 31]
This also re-enables quic-v1 that was disabled due to versioning issues after the release of 0.51.0

[PR 31]: https://github.com/dariusc93/rust-ipfs/pull/31
[PR 32]: https://github.com/dariusc93/rust-ipfs/pull/32

# 0.3.0-alpha.6
- chore: Disable quic-v1 until 0.51 update

# 0.3.0-alpha.5
- fix/: Add debug derive to FDLimit

# 0.3.0-alpha.4
- chore/: Add small delay before processing events [PR 29]

[PR 29]: https://github.com/dariusc93/rust-ipfs/pull/29

# 0.3.0-alpha.3
- refactor/opt: Expanded UninitializedIpfs functionality [PR 21]
- feat/upnp: Use libp2p-nat for handling port forwarding [PR 23]
- refactor/task: Move swarm and events into a task [PR 25]
- chore/: Remove async from event handling functions [PR 27]
- refactor/subscription: Use channels instead of subscription [PR 28]

[PR 23]: https://github.com/dariusc93/rust-ipfs/pull/23
[PR 21]: https://github.com/dariusc93/rust-ipfs/pull/21
[PR 25]: https://github.com/dariusc93/rust-ipfs/pull/25
[PR 27]: https://github.com/dariusc93/rust-ipfs/pull/27
[PR 28]: https://github.com/dariusc93/rust-ipfs/pull/28

# 0.3.0-alpha.2
- fix: Poll receiving oneshot channel directly [PR 20]

[PR 20]: https://github.com/dariusc93/rust-ipfs/pull/20

# 0.3.0-alpha.1
- See commit history