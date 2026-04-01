# Enterprise Design System

## Foundations
- Typography scale:
  - `h3`: Page title and key value headers
  - `h4`: KPI values
  - `h5/h6`: Section and card headers
  - `body1/body2`: Form copy and metadata
- Spacing system:
  - 8pt grid via MUI `theme.spacing(1) = 8px`
  - Primary spacing increments: `8 / 16 / 24 / 32`
- Shape and depth:
  - Card radius `12px`
  - Soft shadow `0 8px 24px rgba(17,33,62,0.08)`
  - Thin semantic border `#E4EBF5`

## Color Tokens
- `primary.main`: `#0F5FFF` (system actions, CTAs)
- `secondary.main`: `#7B2CBF` (accent)
- `success.main`: `#159E66`
- `warning.main`: `#E68A00`
- `error.main`: `#D43D51`
- `background.default`: `#F4F7FB`
- `background.paper`: `#FFFFFF`

## Interaction & Motion
- Framer Motion card reveal:
  - Initial: slight `y` offset + low opacity
  - Animate to settled state within ~240ms
- Hover and focus states:
  - Buttons retain high contrast and subtle lift
  - Inputs use visible focus ring and border shift

## Layout Architecture
- Persistent left sidebar for module navigation
- Top app bar with:
  - Breadcrumbs
  - Role badge
  - User avatar
- Content area:
  - KPI row
  - Analytical charts
  - Workflow forms and data tables
- Responsive behavior:
  - Permanent drawer on desktop
  - Temporary drawer on tablet/mobile

## States
- Loading: linear progress + skeleton blocks
- Empty: dedicated empty-state cards with guidance
- Success: snackbar + inline success alerts
- Error: snackbar + inline error alerts

## Libraries Used
- `@mui/material` for enterprise component system
- `framer-motion` for subtle transitions
- `lucide-react` for iconography
- `recharts` for KPI and trend visualization
