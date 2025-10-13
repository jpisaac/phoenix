# Phoenix Feature Explanations

This directory contains detailed explanations of significant Phoenix features, architectural changes, and JIRA issues. These documents provide comprehensive context, code analysis, and examples to help developers understand complex changes to the codebase.

## Purpose

The `explained/` directory serves as a knowledge base for:
- Major architectural improvements
- Complex feature implementations
- Significant bug fixes that required deep changes
- Performance optimizations
- Design decisions and their rationale

## Documents

### PHOENIX-7707-Explained.md
**Phoenix Server Paging on Valid Rows**

A comprehensive explanation of the architectural change that moved server-side paging from timing on raw HBase rows scanned to timing on valid Phoenix rows returned. This change significantly improved performance for tables with high delete rates and made query behavior more predictable.

**Topics covered:**
- Original server paging design and its limitations
- The PhoenixScannerContext architecture
- Scanner hierarchy and how context flows through the stack
- Before/after code comparisons
- Performance impact and benefits
- Testing strategy

**Related Issues:** PHOENIX-5998, PHOENIX-6207, PHOENIX-6211
**Design Doc:** [Server Paging in Phoenix](../docs/ServerPagingInPhoenix%20(Copy).md)

## Contributing

When adding new explanations to this directory:

1. **File Naming:** Use the format `PHOENIX-XXXX-Explained.md` or `FeatureName-Explained.md`
2. **Structure:** Include:
   - Executive summary
   - Problem statement
   - Solution overview
   - Code examples (before/after when applicable)
   - Impact analysis
   - References (JIRA, design docs, related code)
3. **Context:** Link to related design documents in the `docs/` directory
4. **Code References:** Use file paths and line numbers for specificity
5. **Audience:** Write for developers who will maintain or extend the feature

## Related Directories

- **[docs/](../docs/)**: Design documents and specifications
- **[dev/](../dev/)**: Development tools and IDE configurations
- **[examples/](../examples/)**: Sample code and SQL scripts

## Notes

These explanations complement (but don't replace):
- JIRA issue descriptions
- Design documents in `docs/`
- Code comments in source files
- Commit messages

They provide the "deep dive" analysis that helps developers understand not just **what** changed, but **why** it changed and **how** it works at a detailed level.
