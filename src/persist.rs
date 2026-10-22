//! Carries live ledger state to the repo.
//!
//! The engine does no git work at all, so this module is the only mechanism keeping
//! live state off the local disk. If it stops, the engine keeps operating but state
//! no longer reaches the repository.
//!
//! Deliberately narrow: stages ONLY state/ and README.md, so a half-finished edit
//! anywhere else in the working tree can never be swept into an automated commit.
//! README.md is in scope for documentation purposes; the writer touches only the
//! text between designated markers, but any hand-edits elsewhere in README will
//! ride along, so do not leave README half-edited before a commit.

use std::process::Command;

use crate::paths;

pub const AUTOMSG: &str = "uge-engine: state";
pub const PUSH_EVERY_S: u64 = 1800;
pub const STAGE_PATHS: [&str; 2] = ["state/", "README.md"];

/// The upstream branch this timer pushes to and counts unpushed commits against.
/// Deliberately a fixed `master`, matching the shell script - autodetecting would change
/// WHICH branch live state lands on, which is not a decision an unattended timer should
/// make. If the split repo is ever created with a different default branch, change this
/// constant and the push refspec below together, and expect `upstream_warning` to fire in
/// the meantime.
pub const UPSTREAM_BRANCH: &str = "master";
pub const UPSTREAM_REF: &str = "origin/master";

/// AMEND rather than pile up. State snapshots every 5 minutes produced ~165 commits/day,
/// all "uge-engine: state", which buried real changes and grew .git without bound.
/// Never amend a pushed commit (needs a force-push) and never amend a hand-written one.
pub fn should_amend(head_msg: &str, unpushed: u32) -> bool {
    unpushed > 0 && head_msg.starts_with(AUTOMSG)
}

/// Committing is local and near-free; pushing is the network round trip. The repo is an
/// audit copy synchronized at coarse intervals, so letting it lag costs nothing.
pub fn should_push(now_s: u64, last_push_s: u64) -> bool {
    now_s.saturating_sub(last_push_s) >= PUSH_EVERY_S
}

fn git(args: &[&str]) -> Result<String, String> {
    let out = Command::new("git")
        .current_dir(paths::root())
        .args(args)
        .output()
        .map_err(|e| format!("git {args:?}: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_string());
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// The push-bundling marker the shell script kept at a fixed path. Injectable for the
/// same reason `paths::root` is: a scratch verification run must never rewrite the LIVE
/// timer's marker, which would silently shift the real engine's push schedule.
/// The message emitted when the unpushed-count against the configured upstream fails.
/// Names the ref it tried, so the reader does not have to guess which branch is missing.
pub fn upstream_warning(err: &str) -> String {
    format!(
        "WARNING: `git rev-list --count {UPSTREAM_REF}..HEAD` failed ({err}); \
         treating unpushed as 0, so commit AMENDING IS DISABLED and every tick will stack \
         a new commit. Check that the upstream ref {UPSTREAM_REF} exists \
         (branch `{UPSTREAM_BRANCH}` on remote `origin`)."
    )
}

/// The message emitted when the commit itself fails.
///
/// Names which git invocation was tried, because the amend and plain paths fail for
/// different reasons — an amend can fail on a ref the plain path would not touch — and
/// says plainly that state is staged but not recorded, which is the operator-visible
/// consequence.
pub fn commit_failure(amending: bool, err: &str) -> String {
    let which = if amending { "commit --amend" } else { "commit" };
    format!(
        "ERROR: `git {which}` failed ({err}); live state is STAGED BUT NOT COMMITTED, so \
         nothing is reaching the repo. The engine continues unaffected — this breaks the \
         audit trail, not the core operation. Check that git can resolve an identity \
         (user.email and user.name in git config)."
    )
}

fn last_push_path() -> String {
    std::env::var("UGE_LAST_PUSH").unwrap_or_else(|_| ".uge-last-push".to_string())
}

fn now_s() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn run() -> i32 {
    // State snapshots are recorded here; any publishing of snapshots is handled elsewhere.

    // `git diff --quiet` exits non-zero when there ARE differences, which git() surfaces
    // as Err - the same signal the shell's `&& ... && exit 0` chain used.
    let mut dirty = false;
    for p in STAGE_PATHS {
        if git(&["diff", "--quiet", "--", p]).is_err() {
            dirty = true;
        }
        if git(&["diff", "--cached", "--quiet", "--", p]).is_err() {
            dirty = true;
        }
    }
    if let Ok(s) = git(&["ls-files", "--others", "--exclude-standard", "state/"]) {
        if !s.is_empty() {
            dirty = true;
        }
    }
    if !dirty {
        return 0;
    }

    let mut add = vec!["add", "-A"];
    add.extend(STAGE_PATHS);
    if git(&add).is_err() {
        return 1;
    }
    if git(&["diff", "--cached", "--quiet"]).is_ok() {
        return 0; // nothing staged
    }

    let stamp = crate::now_et().format("%Y-%m-%dT%H:%M:%S%z").to_string();
    let msg = format!("{AUTOMSG} {stamp}");
    let range = format!("{UPSTREAM_REF}..HEAD");
    let unpushed: u32 = match git(&["rev-list", "--count", &range]) {
        Ok(s) => s.parse().unwrap_or(0),
        Err(e) => {
            // Silent-degradation guard. A missing upstream ref (repo created with a `main`
            // default, remote not yet added) makes unpushed read 0, which makes
            // should_amend permanently false - so the timer stacks ~288 commits/day and
            // .git grows without bound, exactly what the amend logic exists to prevent.
            eprintln!("{}", upstream_warning(&e));
            0
        }
    };
    let head_msg = git(&["log", "-1", "--format=%s"]).unwrap_or_default();

    let amending = should_amend(&head_msg, unpushed);
    let commit = if amending {
        git(&["commit", "-q", "--amend", "-m", &msg])
    } else {
        git(&["commit", "-q", "-m", &msg])
    };
    if let Err(e) = commit {
        // Loud and non-zero on purpose. A failed commit must exit non-zero so that
        // a persistence failure is visible in system logs and monitoring, not hidden
        // behind a successful exit code while state silently stops persisting.
        eprintln!("{}", commit_failure(amending, &e));
        return 1;
    }

    let last: u64 = std::fs::read_to_string(last_push_path())
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0);
    if !should_push(now_s(), last) {
        return 0;
    }

    // Rebase onto any interactive work that landed while we were trading, then push.
    if git(&["pull", "--rebase", "-q", "origin", UPSTREAM_BRANCH]).is_err() {
        let _ = git(&["rebase", "--abort"]);
        eprintln!("rebase failed, left committed locally");
        return 1;
    }
    let _ = std::fs::write(last_push_path(), now_s().to_string());
    let refspec = format!("HEAD:{UPSTREAM_BRANCH}");
    if let Err(e) = git(&["push", "-q", "origin", &refspec]) {
        eprintln!("push failed, committed locally only: {e}");
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amends_only_unpushed_auto_commits() {
        assert!(should_amend(
            "uge-engine: state 2031-08-25T12:00:00-0400",
            1
        ));
        assert!(
            !should_amend("uge-engine: state 2031-08-25T12:00:00-0400", 0),
            "already pushed - amending would need a force-push"
        );
        assert!(
            !should_amend("fix: something a human wrote", 3),
            "never amend a hand-written commit"
        );
        assert!(!should_amend("", 1));
    }

    #[test]
    fn push_is_bundled_to_thirty_minutes() {
        assert!(!should_push(1000, 1000));
        assert!(!should_push(1000 + PUSH_EVERY_S - 1, 1000));
        assert!(should_push(1000 + PUSH_EVERY_S, 1000));
        // "No marker file yet" reads as last_push_s = 0. The shell had no special case
        // for it either: with a real epoch clock (~1.7e9) the elapsed span dwarfs 1800s,
        // so a missing marker pushes on the first tick. Asserted with a real-shaped
        // stamp, because the toy pair (1000, 0) is only 1000s apart and must NOT push.
        assert!(
            should_push(1_700_000_000, 0),
            "no recorded push yet - push now"
        );
        assert!(
            !should_push(1000, 0),
            "1000s apart is still inside the bundle window"
        );
    }

    #[test]
    fn push_clock_survives_a_backwards_jump() {
        // saturating_sub, not subtraction: an NTP correction must not panic the timer.
        assert!(!should_push(500, 1000));
    }

    #[test]
    fn upstream_stays_master_and_the_warning_names_the_ref() {
        // Pinned on purpose: changing the pushed branch is a deploy decision, not a
        // refactor. If this ever changes, the systemd timer's target repo changes with it.
        assert_eq!(UPSTREAM_BRANCH, "master");
        assert_eq!(UPSTREAM_REF, "origin/master");

        let w = upstream_warning("fatal: bad revision");
        assert!(
            w.contains("origin/master"),
            "must name the ref it tried: {w:?}"
        );
        assert!(
            w.contains("fatal: bad revision"),
            "must carry git's own error: {w:?}"
        );
        assert!(
            w.to_uppercase().contains("AMENDING IS DISABLED"),
            "must say what silently degraded: {w:?}"
        );
    }

    #[test]
    fn commit_failure_names_the_invocation_and_the_consequence() {
        let plain = commit_failure(false, "fatal: empty ident name");
        assert!(
            plain.contains("`git commit` failed"),
            "must name the invocation: {plain:?}"
        );
        assert!(
            !plain.contains("--amend"),
            "plain path must not claim it amended: {plain:?}"
        );
        assert!(
            plain.contains("fatal: empty ident name"),
            "must carry git's own error: {plain:?}"
        );
        assert!(
            plain.to_uppercase().contains("STAGED BUT NOT COMMITTED"),
            "must say what silently degraded: {plain:?}"
        );

        let amend = commit_failure(true, "fatal: you have nothing to amend");
        assert!(
            amend.contains("`git commit --amend` failed"),
            "amend path must say so - it fails for different reasons: {amend:?}"
        );
    }

    #[test]
    fn stage_paths_are_exactly_two_and_narrow() {
        // Widening this is how a half-edited working tree gets committed automatically.
        assert_eq!(STAGE_PATHS.len(), 2);
        assert!(STAGE_PATHS.contains(&"state/"));
        assert!(STAGE_PATHS.contains(&"README.md"));
        assert!(!STAGE_PATHS.contains(&"."));
        assert!(!STAGE_PATHS.iter().any(|p| p.starts_with("-")));
    }

    // ------------------------------------------------------------------ run()
    // These drive real `git` against a throwaway repo under the OS temp dir.
    // UGE_ROOT and UGE_LAST_PUSH are redirected to scratch locations,
    // so the push branch (network) is never entered.

    fn sh(dir: &std::path::Path, args: &[&str]) -> String {
        let out = Command::new("git")
            .current_dir(dir)
            .args(args)
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn scratch_repo(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("uge-persist-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("state")).unwrap();
        sh(&dir, &["init", "-q", "-b", "master"]);
        sh(&dir, &["config", "user.email", "t@example.invalid"]);
        sh(&dir, &["config", "user.name", "test"]);
        sh(&dir, &["config", "commit.gpgsign", "false"]);
        std::fs::write(dir.join("README.md"), "readme\n").unwrap();
        std::fs::write(dir.join("state/STATUS.md"), "seed\n").unwrap();
        std::fs::write(dir.join("OTHER.md"), "tracked-elsewhere\n").unwrap();
        sh(&dir, &["add", "-A"]);
        sh(&dir, &["commit", "-q", "-m", "seed"]);
        dir
    }

    #[test]
    fn run_stages_only_the_allowlist() {
        let _guard = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = scratch_repo("allowlist");
        let marker = dir.join(".last-push");
        std::fs::write(&marker, now_s().to_string()).unwrap();

        // One in-scope change, one modified TRACKED file out of scope, one untracked.
        std::fs::write(dir.join("state/opportunities.jsonl"), "{}\n").unwrap();
        std::fs::write(dir.join("OTHER.md"), "half-finished human edit\n").unwrap();
        std::fs::write(dir.join("UNRELATED.md"), "scratch\n").unwrap();

        std::env::set_var("UGE_ROOT", dir.to_str().unwrap());
        std::env::set_var("UGE_LAST_PUSH", marker.to_str().unwrap());
        let rc = run();
        std::env::remove_var("UGE_ROOT");
        std::env::remove_var("UGE_LAST_PUSH");

        assert_eq!(rc, 0);
        let head = sh(&dir, &["log", "-1", "--format=%s"]);
        assert!(
            head.starts_with(AUTOMSG),
            "expected an auto-commit, got {head:?}"
        );
        let files = sh(&dir, &["show", "--name-only", "--format=", "HEAD"]);
        assert!(files.contains("state/opportunities.jsonl"));
        assert!(
            !files.contains("OTHER.md"),
            "a modified tracked file outside the allowlist was committed"
        );
        assert!(
            !files.contains("UNRELATED.md"),
            "an untracked file outside the allowlist was committed"
        );
        let status = sh(&dir, &["status", "--porcelain"]);
        assert!(
            status.contains("OTHER.md") && status.contains("UNRELATED.md"),
            "out-of-scope work must be left in the working tree: {status:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_reports_a_failed_commit_instead_of_swallowing_it() {
        // Regression test: a failed commit must not be hidden behind a success exit code,
        // or else persistence failure becomes invisible to monitoring. An empty local identity
        // reproduces commit failure deterministically and portably.
        let _guard = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = scratch_repo("commit-fails");
        let marker = dir.join(".last-push");
        std::fs::write(&marker, now_s().to_string()).unwrap();
        let before = sh(&dir, &["rev-list", "--count", "HEAD"]);

        sh(&dir, &["config", "user.email", ""]);
        sh(&dir, &["config", "user.name", ""]);
        std::fs::write(dir.join("state/opportunities.jsonl"), "{}\n").unwrap();

        std::env::set_var("UGE_ROOT", dir.to_str().unwrap());
        std::env::set_var("UGE_LAST_PUSH", marker.to_str().unwrap());
        let rc = run();
        std::env::remove_var("UGE_ROOT");
        std::env::remove_var("UGE_LAST_PUSH");

        assert_eq!(
            rc, 1,
            "a failed commit must exit non-zero so systemd shows `failed`"
        );
        assert_eq!(
            sh(&dir, &["rev-list", "--count", "HEAD"]),
            before,
            "nothing should have been committed"
        );
        // The work is staged and still on disk - recoverable once git can identify itself.
        let staged = sh(&dir, &["diff", "--cached", "--name-only"]);
        assert!(
            staged.contains("state/opportunities.jsonl"),
            "the change must survive as staged work, not be lost: {staged:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_amends_instead_of_piling_up() {
        let _guard = paths::ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = scratch_repo("amend");
        let marker = dir.join(".last-push");
        std::fs::write(&marker, now_s().to_string()).unwrap();
        std::env::set_var("UGE_ROOT", dir.to_str().unwrap());
        std::env::set_var("UGE_LAST_PUSH", marker.to_str().unwrap());

        std::fs::write(dir.join("state/opportunities.jsonl"), "{}\n").unwrap();
        assert_eq!(run(), 0);
        std::fs::write(dir.join("state/opportunities.jsonl"), "{}\n{}\n").unwrap();
        assert_eq!(run(), 0);
        // No origin/master here, so unpushed parses as 0 and the shell's rule says
        // COMMIT, not amend - a commit with no upstream is treated as pushed-safe.
        let n = sh(&dir, &["rev-list", "--count", "HEAD"]);
        assert_eq!(n, "3", "no upstream: each tick commits (matches the shell)");

        // With an upstream that is behind HEAD, the tip is amended instead.
        let bare = dir.join("bare.git");
        sh(&dir, &["init", "-q", "--bare", bare.to_str().unwrap()]);
        sh(&dir, &["remote", "add", "origin", bare.to_str().unwrap()]);
        sh(&dir, &["push", "-q", "origin", "HEAD:master"]);
        std::fs::write(dir.join("state/opportunities.jsonl"), "{}\n{}\n{}\n").unwrap();
        assert_eq!(run(), 0);
        let before = sh(&dir, &["rev-list", "--count", "HEAD"]);
        assert_eq!(
            before, "4",
            "first tick after a push must commit, not amend"
        );
        std::fs::write(dir.join("state/opportunities.jsonl"), "{}\n{}\n{}\n{}\n").unwrap();
        assert_eq!(run(), 0);
        assert_eq!(
            sh(&dir, &["rev-list", "--count", "HEAD"]),
            "4",
            "an unpushed auto-commit must be amended, not stacked"
        );

        // Nothing changed at all -> no commit.
        assert_eq!(run(), 0);
        assert_eq!(sh(&dir, &["rev-list", "--count", "HEAD"]), "4");

        std::env::remove_var("UGE_ROOT");
        std::env::remove_var("UGE_LAST_PUSH");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
