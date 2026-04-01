import {
  AppBar,
  Avatar,
  Box,
  Breadcrumbs,
  Button,
  Chip,
  CssBaseline,
  Divider,
  Drawer,
  IconButton,
  List,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Stack,
  Tooltip,
  Toolbar,
  Typography,
} from '@mui/material';
import { LogOut, Menu, Moon, Sun } from 'lucide-react';

export default function AppLayout({
  appName,
  appTagline,
  drawerWidth,
  isMobile,
  mobileOpen,
  onOpenMobileNav,
  onCloseMobileNav,
  colorMode,
  onToggleColorMode,
  breadcrumb,
  user,
  checkInOnly,
  nav,
  activeView,
  onNavigate,
  onRequestLogout,
  children,
}) {
  return (
    <Box className={`app-root mode-${colorMode}`}>
      <CssBaseline />
      <AppBar
        data-testid="app-header"
        position="sticky"
        color="inherit"
        elevation={0}
        sx={{
          top: 0,
          borderBottom: '1px solid',
          borderColor: 'divider',
          ml: { lg: `${drawerWidth}px` },
          width: { lg: `calc(100% - ${drawerWidth}px)` },
          height: 'var(--app-header-h)',
          zIndex: (theme) => theme.zIndex.drawer + 2,
        }}
      >
        <Toolbar sx={{ justifyContent: 'space-between', minHeight: 'var(--app-header-h) !important' }}>
          <Stack direction="row" spacing={1} alignItems="center">
            {isMobile && (
              <IconButton aria-label="Open navigation menu" onClick={onOpenMobileNav}>
                <Menu size={18} />
              </IconButton>
            )}
            <Breadcrumbs>
              <Typography color="text.secondary">Operations</Typography>
              <Typography>{breadcrumb}</Typography>
            </Breadcrumbs>
          </Stack>
          <Stack direction="row" spacing={1} alignItems="center">
            <Tooltip title={colorMode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}>
              <IconButton aria-label="Toggle dark mode" onClick={onToggleColorMode}>
                {colorMode === 'dark' ? <Sun size={18} /> : <Moon size={18} />}
              </IconButton>
            </Tooltip>
            {checkInOnly && <Chip size="small" color="info" label="Check-In Only" />}
            <Chip size="small" color="primary" label={user?.role} />
            <Avatar>{(user?.name || 'U').slice(0, 1)}</Avatar>
          </Stack>
        </Toolbar>
      </AppBar>

      <Drawer
        variant={isMobile ? 'temporary' : 'permanent'}
        open={isMobile ? mobileOpen : true}
        onClose={onCloseMobileNav}
        sx={{
          '& .MuiDrawer-paper': {
            width: drawerWidth,
            borderRight: '1px solid',
            borderColor: 'divider',
            boxSizing: 'border-box',
            top: { lg: 'var(--app-header-h)' },
            height: { lg: 'calc(100dvh - var(--app-header-h))' },
            zIndex: (theme) => (isMobile ? theme.zIndex.modal : theme.zIndex.drawer),
          },
        }}
      >
        <Box sx={{ p: 2 }}>
          <Stack direction="row" spacing={1.2} alignItems="center" sx={{ mb: 1 }}>
            <Avatar src="/brand-logo.svg" variant="rounded" alt={`${appName} logo`} />
            <Typography variant="h6">{appName}</Typography>
          </Stack>
          <Typography variant="body2" color="text.secondary">{appTagline}</Typography>
        </Box>
        <Divider />
        <List sx={{ p: 1 }}>
          {nav.map((item) => {
            const Icon = item.icon;
            return (
              <ListItemButton
                key={item.id}
                selected={activeView === item.id}
                onClick={() => onNavigate(item.id)}
                sx={{ borderRadius: 2, mb: 0.5 }}
              >
                <ListItemIcon sx={{ minWidth: 36 }}><Icon size={18} /></ListItemIcon>
                <ListItemText primary={item.label} />
              </ListItemButton>
            );
          })}
        </List>
        <Box sx={{ mt: 'auto', p: 2 }}>
          <Button
            fullWidth
            color="error"
            variant="outlined"
            startIcon={<LogOut size={16} />}
            onClick={onRequestLogout}
          >
            Logout
          </Button>
        </Box>
      </Drawer>

      <Box
        data-testid="app-main"
        className="app-shell-main"
        sx={{
          ml: { lg: `${drawerWidth}px` },
          display: 'block',
        }}
      >
        {children}
      </Box>
    </Box>
  );
}
