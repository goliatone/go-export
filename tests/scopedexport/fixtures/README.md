# Synthetic fixtures

Group A: aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa. Group B: bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb.

User 1 has read, write and endpoint.manage in A. User 2 has read in B; the concurrent endpoint test explicitly adds endpoint.manage in B. User 3 has read in A and write in B. No tenant/org substitution occurs.

Resource A and Resource B have randomly generated IDs and persisted ownership. Two randomly identified endpoints belong to A; one belongs to B. Passwords/signing keys are ephemeral random synthetic values and are not saved. All payloads are opaque test strings. Fixtures are created by support_test.go; no customer/application data or development database is used.
