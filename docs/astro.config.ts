import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightBlog from 'starlight-blog';
import starlightLlmActions from 'starlight-llm-actions';
import { satteri } from '@astrojs/markdown-satteri';
import { siteKitAstro } from '@cachix/site-kit/astro';
import { siteKitStarlight } from '@cachix/site-kit/starlight';
import { siteBlogOptions } from '@cachix/site-kit/starlight/blog';
import { siteLlmActionsOptions } from '@cachix/site-kit/starlight/llms';
import { hideRustdocLines } from './src/remark/hide-rustdoc-lines';

export default defineConfig({
  site: 'https://casita.rs/',
  markdown: { processor: satteri({ mdastPlugins: [hideRustdocLines] }) },
  integrations: [
    siteKitAstro({
      github: {
        repository: 'cachix/casita',
      },
    }),
    starlight({
      plugins: [
        siteKitStarlight(),
        starlightBlog(siteBlogOptions()),
        starlightLlmActions(siteLlmActionsOptions(`Casita is a pre-release verified content-addressed repository for immutable object graphs. Repository<PS, SS> combines physical payloads with revisioned immutable object records and named roots. Filesystem, IPLD, native Git, and custom formats keep their native identity while sharing one publication, synchronization, collection, and integrity model.

Start with: cargo install --path crates/casita; casita --repository ./cache import ./project --root projects/demo; casita sync --from ./cache --to ./mirror --root projects/demo.

The supported Rust API uses the non-generic casita::Repository: Repository::local(path).await opens local storage and Repository::memory() opens ephemeral storage. Custom payload/state backends use casita::experimental::Repository::new(payloads, state) with the experimental Cargo feature.`)),
      ],
      title: 'Casita',
      description: 'A home for your objects. Verified content-addressed storage, synchronization, and garbage collection, built in Rust.',
      favicon: '/favicon.svg',
      components: {
        SiteTitle: './src/overrides/SiteTitle.astro',
      },
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/cachix/casita' },
        { icon: 'discord', label: 'Discord', href: 'https://discord.gg/naMgvexb6q' },
      ],
      editLink: { baseUrl: 'https://github.com/cachix/casita/edit/main/docs/' },
      lastUpdated: true,
      customCss: ['./src/styles/custom.css', './src/styles/landing.css'],
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'What is Casita?', slug: 'overview' },
            { label: 'Quick Start', slug: 'getting-started' },
            { label: 'CLI', slug: 'cli' },
            { label: 'Library', slug: 'library' },
          ],
        },
        {
          label: 'Importing',
          items: [
            { label: 'Filesystem', slug: 'guides/filesystem' },
            { label: 'Tar Archive', slug: 'guides/tar' },
            { label: 'Native Git', slug: 'guides/git' },
            { label: 'Casitar Archive', slug: 'guides/casitar' },
            { label: 'Adding a New Importer', slug: 'guides/adding-an-importer' },
          ],
        },
        {
          label: 'Workflows',
          items: [
            { label: 'Run Applications', slug: 'guides/run' },
            { label: 'Synchronization', slug: 'guides/sync' },
            { label: 'Operations', slug: 'guides/operations' },
            { label: 'S3 Maintenance', slug: 'guides/s3-maintenance' },
            { label: 'Multi-Owner S3', slug: 'guides/s3-multi-owner' },
          ],
        },
        {
          label: 'Integrations',
          items: [
            { label: 'Cargo Prototype', slug: 'integrations/cargo' },
            { label: 'Local IPC', slug: 'integrations/ipc' },
          ],
        },
        {
          label: 'Concepts',
          items: [
            { label: 'Overview', slug: 'concepts' },
            { label: 'Division of Responsibility', slug: 'concepts/responsibilities' },
            { label: 'Repository', slug: 'concepts/repository' },
            { label: 'Verification', slug: 'concepts/verification' },
            { label: 'Roots & Retention', slug: 'concepts/roots-and-retention' },
            { label: 'Import Semantics', slug: 'concepts/imports' },
            { label: 'Sync', slug: 'concepts/sync' },
            { label: 'Garbage Collection', slug: 'concepts/garbage-collection' },
            { label: 'Blob Storage', slug: 'concepts/blob-storage' },
            { label: 'Directory Format', slug: 'concepts/directory-storage' },
            { label: 'Deduplication', slug: 'concepts/deduplication' },
          ],
        },
        { label: 'Extending', items: [{ label: 'Custom Object Formats', slug: 'guides/custom-formats' }] },
        {
          label: 'Reference',
          items: [
            { label: 'Overview', slug: 'reference' },
            { label: 'CLI', slug: 'reference/cli' },
            { label: 'Rust API', slug: 'reference/rust-api' },
            { label: 'Experimental Rust API', slug: 'reference/experimental-rust-api' },
            { label: 'Identifiers', slug: 'reference/identifiers' },
            { label: 'Object Formats', slug: 'reference/object-formats' },
            { label: 'Cargo Features', slug: 'reference/cargo-features' },
            { label: 'Local Repository', slug: 'reference/local-repository' },
            { label: 'Benchmarks', slug: 'reference/benchmarks' },
            { label: 'Errors & Integrity', slug: 'reference/errors' },
            { label: 'Reliability Contract', slug: 'reference/reliability' },
            {
              label: 'Rust API source',
              link: 'https://github.com/cachix/casita/blob/main/crates/casita/src/lib.rs',
              attrs: { target: '_blank' },
            },
          ],
        },
      ],
    }),
  ],
});
