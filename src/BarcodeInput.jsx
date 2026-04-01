import { useCallback, useEffect, useRef } from 'react';
import { TextField } from '@mui/material';

const playTone = (frequency, durationMs) => {
  if (typeof window === 'undefined') return;
  const AudioCtx = window.AudioContext || window.webkitAudioContext;
  if (!AudioCtx) return;

  const context = new AudioCtx();
  const oscillator = context.createOscillator();
  const gainNode = context.createGain();

  oscillator.type = 'sine';
  oscillator.frequency.value = frequency;
  gainNode.gain.setValueAtTime(0.0001, context.currentTime);
  gainNode.gain.exponentialRampToValueAtTime(0.14, context.currentTime + 0.01);
  gainNode.gain.exponentialRampToValueAtTime(0.0001, context.currentTime + durationMs / 1000);

  oscillator.connect(gainNode);
  gainNode.connect(context.destination);
  oscillator.start();
  oscillator.stop(context.currentTime + durationMs / 1000);
  oscillator.onended = () => context.close().catch(() => {});
};

const playSuccess = () => {
  playTone(980, 90);
};

const playFailure = () => {
  playTone(220, 150);
};

export default function BarcodeInput({
  label,
  value,
  onChange,
  onScan,
  autoFocus = false,
  autoFocusKey = '',
  duplicateWindowMs = 300,
  clearOnSuccess = true,
  disabled = false,
  error = false,
  helperText = '',
  placeholder = '',
}) {
  const inputRef = useRef(null);
  const lastScanRef = useRef({ value: '', ts: 0 });

  useEffect(() => {
    if (!autoFocus) return;
    const timer = window.setTimeout(() => {
      if (inputRef.current) {
        inputRef.current.focus();
        inputRef.current.select?.();
      }
    }, 20);
    return () => window.clearTimeout(timer);
  }, [autoFocus, autoFocusKey]);

  const handleKeyDown = useCallback(async (event) => {
    if (event.key !== 'Enter') return;

    const candidate = String(event.currentTarget?.value || '').trim();
    if (!candidate) return;

    const now = Date.now();
    const previous = lastScanRef.current;
    if (previous.value === candidate && now - previous.ts < duplicateWindowMs) {
      event.preventDefault();
      return;
    }

    if (!onScan) return;

    event.preventDefault();
    let ok = false;
    try {
      ok = Boolean(await onScan(candidate));
    } catch {
      ok = false;
    }

    if (ok) {
      lastScanRef.current = { value: candidate, ts: now };
      playSuccess();
      if (clearOnSuccess) onChange('');
    } else {
      playFailure();
    }
  }, [clearOnSuccess, duplicateWindowMs, onChange, onScan]);

  return (
    <TextField
      label={label}
      value={value}
      inputRef={inputRef}
      autoFocus={autoFocus}
      onChange={(event) => onChange(event.target.value)}
      onKeyDown={handleKeyDown}
      error={error}
      helperText={helperText}
      placeholder={placeholder || 'Scan barcode...'}
      disabled={disabled}
      fullWidth
      InputProps={{
        sx: {
          fontSize: '1.2rem',
          fontWeight: 600,
          minHeight: 58,
          '& input': {
            letterSpacing: '0.02em',
            py: 1.25,
          },
        },
      }}
      inputProps={{
        'aria-label': label,
        autoCapitalize: 'off',
        autoCorrect: 'off',
        spellCheck: 'false',
      }}
    />
  );
}
