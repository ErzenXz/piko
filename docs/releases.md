# Releases

Releases use npm staged publishing with GitHub OIDC provenance and maintainer
passkey approval. This preserves the approval recorded on 0.4.0, which pnpm
ranks above unapproved automated publishing. Approved releases starting with
0.7.1 support ordinary `pnpm add -g @erzenx/piko` with no trust exclusions.

1. Bump Cargo, root and launcher versions together, including all six exact
   optional dependency pins. Push the reviewed version tag.
2. The Release workflow tests six native targets and stages all seven tarballs
   with provenance. Its `staged-release` artifact preserves exact payloads and
   `piko-staged.json`: source commit, names, versions, stage IDs and SHA-512 hashes.
3. Review the successful run and receipt. With a passkey, open
   [npm's staged packages queue](https://www.npmjs.com/settings/erzenx/staged-packages)
   and approve all six native packages, then the launcher. npm may require a
   separate passkey verification per package. Match names, versions and hashes
   with the receipt; only a maintainer performs this step.

   The CLI also accepts the receipt's seven IDs together:

   ```sh
   pnpm stage approve ID1 ID2 ID3 ID4 ID5 ID6 ID7 --registry=https://registry.npmjs.org/
   ```

   Sign in with `pnpm login` first if needed. With passkey-only authentication,
   the CLI may approve the first package and then ask for an authenticator OTP;
   finish the remaining approvals in the browser rather than guessing a code.
   CLI dependency ordering publishes native packages before the launcher.
   Approval becomes available after malware scanning completes. CI cannot
   supply a maintainer's proof of presence.
4. Run Release on the same tag with `phase=finalize` and `staged_run_id` set to
   the successful staging run. It downloads the original artifacts without
   rebuilding, checks public approval/provenance/integrity, publishes checksummed
   GitHub downloads, and tests fresh public installs on all six platforms with
   `trustPolicy: no-downgrade` and no trust exclusions.

Install tests retain pnpm's default release-age policy. A user who separately
enables a strict quarantine must still wait for that chosen release age.

## Interrupted releases

Keep the artifact and receipt. An upload intent without a confirmed stage ID
means the outcome is unknown: reconcile using authenticated `pnpm stage list`,
`view` and `download`, comparing downloaded integrity before recovering its ID.
Never blindly upload again or delete an npm version. Rejected stages need a
fresh version. npm visibility may lag approval; wait instead of republishing.
For a partially approved batch, continue approving the remaining staged IDs;
already published versions must not be staged again.

Finalization accepts only a successful staging run for the same source commit,
workflow and tag. It refuses absent approval, hash or provenance mismatches,
never publishes to npm, and preserves verified existing GitHub assets.

Scripts validate registry metadata and attestation claims; they do not perform
independent cryptographic Sigstore verification or replace a source audit.
