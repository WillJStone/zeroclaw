Check for upstream changes to the ZeroClaw repo and generate a sync report.

You are running in headless mode, invoked by ZeroClaw as a tool. Your output will be relayed to the user via Telegram. Be concise and structured.

## Steps

1. Make sure you're in the zeroclaw repo at /home/wstone/Documents/zeroclaw

2. Check which branch is active. It should be `local`. If it's `main`, warn and stop.

3. Fetch upstream:
   ```
   git fetch origin
   ```

4. Count new upstream commits since the local branch diverged:
   ```
   git log local..origin/main --oneline
   ```
   If zero new commits, report "No upstream changes since last sync" and stop.

5. Summarize what changed upstream:
   ```
   git log local..origin/main --oneline --stat
   ```
   Group changes by area (providers, channels, tools, config, etc.) and highlight anything that touches files you've modified locally.

6. Check your local changes (files modified on the `local` branch vs `main`):
   ```
   git diff main..local --name-only
   ```

7. Dry-run merge to detect conflicts:
   ```
   git stash
   git checkout -b merge-test origin/main
   git merge local --no-commit --no-ff
   ```
   Check the result. Then clean up:
   ```
   git merge --abort
   git checkout local
   git branch -D merge-test
   git stash pop
   ```

8. Generate report in this format:

   ```
   ZEROCLAW UPSTREAM SYNC REPORT

   New upstream commits: <count>
   Period: <oldest commit date> to <newest commit date>

   UPSTREAM CHANGES BY AREA:
   - <area>: <brief summary> (<N> files)
   - ...

   YOUR LOCAL FILES:
   - <file>: <what you changed>
   - ...

   CONFLICT STATUS: <Clean merge / Conflicts detected>

   [If conflicts:]
   CONFLICTING FILES:
   - <file>: <why it conflicts>
   - ...

   OPTIONS:
   A) Merge now (clean/no conflicts)
   B) Merge with manual conflict resolution needed
   C) Skip this update
   D) Cherry-pick specific upstream commits only
   ```

Keep the report compact. No fluff. This gets sent via Telegram so brevity matters.
