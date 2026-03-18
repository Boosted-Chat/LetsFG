---
hide:
  - toc
---

# LetsFG

**Flight search & booking for AI agents and developers.**
400+ airlines, 102 direct airline connectors, virtual interlining — straight from the terminal.

<div class="install-cmd"><span class="prompt">$</span> pip install letsfg</div>

<div class="hero-actions">
<a href="getting-started/" class="btn-primary">Get started</a>
<a href="api-guide/" class="btn-ghost">API guide</a>
<a href="https://api.letsfg.co/docs" class="btn-ghost" target="_blank">Swagger ↗</a>
<a href="https://smithery.ai/server/letsfg-mcp" class="btn-ghost" target="_blank">Smithery ↗</a>
</div>

---

<div class="cards-grid">

<a class="card" href="getting-started/">
<span class="card-icon">🚀</span>
<strong>Quickstart</strong>
<p>Install and search in one command — no API key needed. Or register for full 400+ airline access.</p>
</a>

<a class="card" href="api-guide/">
<span class="card-icon">⚡</span>
<strong>API Guide</strong>
<p>Search results, error handling, workflows, unlock mechanics.</p>
</a>

<a class="card" href="agent-guide/">
<span class="card-icon">🤖</span>
<strong>AI Agent Guide</strong>
<p>Architecture, preference scoring, rate limits, price tracking.</p>
</a>

<a class="card" href="cli-reference/">
<span class="card-icon">⌨️</span>
<strong>CLI Reference</strong>
<p>Commands, flags, cabin codes — full terminal reference.</p>
</a>

<a class="card" href="architecture-guide/">
<span class="card-icon">🏗️</span>
<strong>Architecture Guide</strong>
<p>Parallel execution, failure isolation, caching, browser concurrency, performance optimization.</p>
</a>

<a class="card" href="tutorials/">
<span class="card-icon">📚</span>
<strong>Tutorials</strong>
<p>Python & JS integration patterns, concurrent search, building travel assistants.</p>
</a>

<a class="card" href="packages/">
<span class="card-icon">📦</span>
<strong>Packages & SDKs</strong>
<p>Python SDK, JavaScript SDK, MCP Server for Claude & Cursor.</p>
</a>

<a class="card" href="https://api.letsfg.co/docs" target="_blank">
<span class="card-icon">📄</span>
<strong>OpenAPI Reference</strong>
<p>Interactive Swagger docs — try every endpoint in your browser.</p>
</a>

</div>

---

## Two ways to use

| | **Local Only** (no API key) | **With API Key** (recommended) |
|---|---|---|
| Install | `pip install letsfg` | `pip install letsfg` |
| Setup | Nothing | `letsfg register --email you@example.com` |
| Airlines | 102 via local connectors | 102 local + 400+ via GDS/NDC |
| Price | Free | Free — star GitHub repo for access |
| Coverage | LCCs + major carriers with public APIs | Full global coverage including premium carriers |

## How it works

<div class="flow">
<span class="flow-step">Search <small>free</small></span>
<span class="flow-arrow">→</span>
<span class="flow-step">Unlock <small>free</small></span>
<span class="flow-arrow">→</span>
<span class="flow-step">Book <small>free</small></span>
</div>

1. **Search** — real-time offers with price, airlines, duration, stopovers. Free and unlimited.
2. **Unlock** — confirms the live price with the airline, reserves for 30 min. Free with GitHub star.
3. **Book** — creates real PNR. E-ticket sent to passenger email. Free after unlock.
