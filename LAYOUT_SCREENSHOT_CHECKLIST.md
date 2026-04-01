# Layout Screenshot Checklist

## Before Fix (capture evidence)
- [ ] Login complete, dashboard first render: header covers top of KPI cards.
- [ ] Switch from `Dashboard` -> `Issue`: content starts mid-way or with incorrect vertical offset.
- [ ] Mobile/tablet width: drawer/header overlap with first form field.
- [ ] After navigating tabs several times, page opens at prior scroll position instead of top.

## After Fix (validate regression)
- [ ] Dashboard top card starts fully below header (no clipping).
- [ ] `Check-In`, `Stock In`, `Issue`, `Return`, `Cycle Count`, `Print Queue`, `AI Assistant` each open at top.
- [ ] Header remains fixed and visible while content is never hidden behind it.
- [ ] Desktop: permanent drawer starts below header, does not collide with top bar.
- [ ] Tablet/mobile: temporary drawer and header stack correctly; main content is aligned.
- [ ] No horizontal overflow at any page breakpoint.
- [ ] Scroll restoration works when switching tabs (content resets to top).
