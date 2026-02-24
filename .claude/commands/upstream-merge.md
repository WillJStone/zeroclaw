Merge upstream changes into the local branch and rebuild ZeroClaw.

You are running in headless mode, invoked by ZeroClaw as a tool. Your output will be relayed to the user via Telegram. Be concise.

## Steps

1. Make sure you're in /home/wstone/Documents/zeroclaw on the `local` branch.

2. Fetch latest:
   ```
   git fetch origin
   ```

3. Rebase local onto upstream:
   ```
   git rebase origin/main
   ```

4. If rebase conflicts:
   - Report which files conflict and the nature of each conflict
   - Do NOT attempt to resolve automatically
   - Run `git rebase --abort` to leave the branch clean
   - Report back: "Rebase failed due to conflicts in: <files>. Manual resolution needed."
   - Stop here

5. If rebase succeeds, rebuild:
   ```
   cargo build --release
   ```

6. If build fails:
   - Report the error
   - Do NOT revert automatically
   - Stop here

7. Run tests:
   ```
   cargo test 2>&1 | grep "^test result:"
   ```

8. Push updated local branch to fork:
   ```
   git push mine local --force-with-lease
   ```

9. Report:
   ```
   MERGE COMPLETE

   Rebased onto: <upstream commit hash>
   Build: OK
   Tests: <X> passed, <Y> failed

   [If test failures:]
   FAILING TESTS:
   - <test name>: <brief error>

   Binary at target/release/zeroclaw is updated (symlinked).
   Restart the daemon to pick up changes.
   ```
