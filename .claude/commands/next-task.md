Read @docs/milestones/README.md and all milestone files in @docs/milestones/ (M1 through M5).

Identify the single highest-priority uncompleted task using this priority order:
1. Tasks tagged BLOCKER (other tasks depend on them)
2. Tasks in the lowest-numbered Milestone that has incomplete tasks
3. Within that Milestone, tasks in the lowest-numbered Epic
4. Within that Epic, the first uncompleted task in order

Print the result in this exact format:

**Next Task:** <task description>
**Story:** <parent story>
**Epic:** <epic name>
**Milestone:** <milestone name>
**Files to modify:** <comma-separated file paths>
**Hint:** <one sentence on how to implement it>

Then check if there are any follow-on tasks in the same Epic that are blocked by this task,
and list them under "Unlocks:" so the developer knows what becomes available next.
