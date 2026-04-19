Read all files in @docs/milestones/ (README.md plus M1 through M5).

For each Milestone, count total tasks (lines with `- [ ]` or `- [x]`) and completed tasks
(lines with `- [x]`). Print a progress table in this format:

| Milestone | Done | Total | % |
|-----------|------|-------|---|
| M1: Core Infrastructure | X | Y | Z% |
| ...

After the table, scan all milestone files for lines containing the word BLOCKER.
List each blocker under a "## Active Blockers" heading with its Milestone, Epic, and task text.
If there are no blockers, print "No active blockers."
