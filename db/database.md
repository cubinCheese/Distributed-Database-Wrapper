# DB blueprint

## Big Picture
Dataset is sharded in to **Original Languages**:
1. Chinese (zh)
2. Japanese (ja)
3. Korean (ko)
4. Malaysian (ms)
5. Filipino (fil)
6. Indonesian (id)
7. Khmer (km)
8. Thai (th)

## Misc
- Left as modular as possible for future implementation of Leader Election/Raft config settings
- Each language is expected to be replicated >2 once Raft is implemented
- Algorithms for quick lookup will be implemented given enough time
