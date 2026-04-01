# Visual Hierarchy

## 1) Navigation Layer
- Left sidebar is persistent on desktop and modal on tablet/mobile.
- Primary workflows are ordered by operational sequence:
  1. Dashboard
  2. Check-In
  3. Stock In
  4. Issue
  5. Return
  6. Cycle Count
  7. Print Queue
  8. AI Assistant

## 2) Context Layer
- Top bar keeps user context always visible:
  - Breadcrumb (current location)
  - Role badge
  - User avatar

## 3) Decision Layer
- KPI cards are top-priority visual anchors:
  - Total Inventory Value
  - Active Encounters
  - Daily Usage
  - Low Stock Alerts
- These cards are followed by trends and composition charts to drive quick decisions.

## 4) Execution Layer
- Each workflow page follows the same pattern:
  - Form/action controls first
  - Validation and state feedback second
  - Result tables/outputs last

## 5) State Feedback Layer
- Loading:
  - Global linear progress + component skeletons
- Success:
  - Snackbar + inline success alert
- Error:
  - Inline error alert + snackbar
- Empty:
  - Dedicated empty-state cards with next-step guidance

## 6) Mobile/iPad Strategy
- Drawer collapses to temporary mode under tablet width.
- Cards and form groups stack vertically.
- Control spacing and target sizes remain touch-friendly using 8pt spacing.
