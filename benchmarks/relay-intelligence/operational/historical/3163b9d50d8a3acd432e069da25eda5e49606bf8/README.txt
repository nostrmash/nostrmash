Historical measurements from implementation 3163b9d50d8a3acd432e069da25eda5e49606bf8.
The benchmark run started at merge commit ed19d29d926e8692f7cc8a3df3424caef0f9fab4.
Result files were committed in 89b3add76abd1eaf0dd386fe2edb2bc445a111b9.
The report that quotes them was updated in ccb50a633a546c10d01291f8ccd22637c634a279.

These measurements predate the retention-index and membership-metadata commit
31df4c808129f633593d7bcd605cb178dad40727 and the completion-digest commit
caaef7b2ad005ca56c17ecf288c72d9f169f58b3. The 870 ms and 7,995 ms purge
figures are the unindexed account-scale baseline. Do not relabel this
directory as the final indexed implementation.
