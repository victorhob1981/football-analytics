```markdown
# Design System Specification: High-End Football Analytics

## 1. Overview & Creative North Star: "The Tactical Lens"
The Creative North Star for this design system is **"The Tactical Lens."** We are moving away from the cluttered, "spreadsheet-heavy" aesthetic of traditional sports betting sites and toward a high-end, editorial analytics platform. 

The goal is to present complex data with the authority of a premium sports publication and the precision of a professional scouting tool. We achieve this through **Intentional Asymmetry**—using larger display typography to anchor data-dense tables—and **Tonal Depth**, replacing rigid borders with soft, layered surfaces that feel like a physical stack of scouting reports.

## 2. Colors & Surface Philosophy
The palette is built on a foundation of neutral slates, punctuated by a signature "Deep Emerald" (`primary`) to denote success, momentum, and elite performance.

### The "No-Line" Rule
**Explicit Instruction:** Do not use 1px solid borders to define sections. Layout boundaries must be established solely through background color shifts. For example, a `surface-container-low` sidebar should sit against a `surface` main content area. This creates a more sophisticated, "built-in" feel rather than a "boxed-in" one.

### Surface Hierarchy & Nesting
Treat the UI as a series of physical layers. Use the following tiers to define importance:
- **Base Level:** `surface` (#f9f9ff) for the main application background.
- **Sectioning:** `surface-container-low` (#f0f3ff) to group related data modules.
- **Interactive Cards:** `surface-container-lowest` (#ffffff) to make data "pop" forward.
- **Active/High Importance:** `surface-container-highest` (#d8e3fb) for active tabs or hovered states.

### The "Glass & Gradient" Rule
To elevate the dashboard beyond a standard grid, floating elements (like player profile modals or quick-action menus) should utilize **Glassmorphism**. Apply `surface-variant` at 70% opacity with a `backdrop-filter: blur(12px)`. 

For Primary CTAs and Hero stats, use a subtle linear gradient:
- **Direction:** 135deg
- **From:** `primary` (#003526) 
- **To:** `primary_container` (#004e39)
This adds "visual soul" and depth that flat hex codes cannot achieve.

## 3. Typography: The Editorial Scale
We use a dual-font approach to balance personality with extreme legibility.

*   **Display & Headlines (Manrope):** Used for "Brand Moments"—league titles, player names, and high-level summaries. Its geometric nature feels modern and authoritative.
*   **Body & Labels (Inter):** The workhorse for data. Chosen for its high x-height and exceptional legibility in dense tables and small captions.

**Typography Hierarchy:**
- **Display-LG (3.5rem):** Used for key metrics (e.g., "Expected Goals" total).
- **Headline-SM (1.5rem):** Used for module titles (e.g., "Player Heatmap").
- **Title-SM (1rem):** Used for table headers; always use `secondary` slate gray for these.
- **Label-SM (0.6875rem):** Used for "Coverage State" badges and metadata.

## 4. Elevation & Depth
Hierarchy is conveyed through **Tonal Layering** rather than structural lines.

- **The Layering Principle:** Place a `surface-container-lowest` card inside a `surface-container-low` section. This creates a soft, natural lift.
- **Ambient Shadows:** Only use shadows for floating elements (Tooltips, Popovers).
    - **Specs:** `0px 8px 24px` with 6% opacity using `on-surface` (#111c2d). This mimics natural light.
- **The "Ghost Border" Fallback:** If a divider is functionally required for accessibility, use the `outline-variant` token (#bfc9c3) at **15% opacity**. Never use 100% opaque borders.

## 5. Components & Data Patterns

### Coverage-State Indicators (Badges)
Crucial for football analytics (Full vs. Partial coverage). 
- **Full Coverage:** `primary_fixed` background with `on_primary_fixed_variant` text.
- **Partial Coverage (Amber):** `tertiary_fixed` (#ffdcc3) background with `on_tertiary_fixed_variant` (#6e3900) text.
- **Shape:** Use `rounded-full` for badges to contrast against the `md` (0.375rem) corners of cards.

### Data Cards & Lists
- **Rule:** Forbid the use of horizontal divider lines.
- **Spacing:** Use `spacing-5` (1.1rem) to separate list items.
- **Interaction:** On hover, shift the background from `surface-container-lowest` to `surface-container-high`.

### Buttons
- **Primary:** Gradient fill (`primary` to `primary-container`), `rounded-md`, white text.
- **Secondary:** Transparent background with a "Ghost Border" (15% opacity `outline-variant`).
- **Tertiary:** Text-only using `secondary` slate gray; shifts to `primary` on hover.

### Analytics Tables
- **Header:** `surface-container-low` with `label-md` uppercase typography.
- **Numbers:** Use tabular numbers (monospaced) for Inter to ensure player stats align vertically.
- **Success Markers:** Use `primary` (#003526) for positive trend arrows or high-percentile scores.

## 6. Do's and Don'ts

### Do
- **Do** use negative space (`spacing-8` and above) to separate major modules.
- **Do** use `primary_fixed_dim` for subtle emerald accents in icons.
- **Do** use `tertiary` (amber/orange) sparingly—only for status alerts or "Partial Coverage" warnings.
- **Do** leverage the `surface-tint` (#1b6b51) at low opacity (5%) for large background sections to give the "white" a sophisticated green undertone.

### Don't
- **Don't** use pure black (#000000) for text. Always use `on_surface` or `secondary` slate grays.
- **Don't** use sharp 90-degree corners. Stick to the `md` (0.375rem) or `lg` (0.5rem) scale for a premium feel.
- **Don't** use standard "Drop Shadows" on cards. Rely on background color shifts (`surface` tiers).
- **Don't** use more than two colors in a single data visualization. Stick to the emerald-to-slate scale.