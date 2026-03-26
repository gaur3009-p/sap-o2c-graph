/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ['./src/**/*.{js,ts,jsx,tsx,mdx}'],
  theme: {
    extend: {
      fontFamily: {
        mono: ['"JetBrains Mono"', '"Fira Code"', 'monospace'],
        sans: ['"DM Sans"', 'sans-serif'],
        display: ['"Syne"', 'sans-serif'],
      },
      colors: {
        ink: {
          950: '#060810',
          900: '#0c1118',
          800: '#111827',
          700: '#1a2433',
          600: '#243044',
        },
        acid: {
          DEFAULT: '#00ff88',
          dim:     '#00cc6a',
          muted:   '#00ff8820',
        },
        amber: {
          flow: '#f59e0b',
        },
        node: {
          customer:  '#6366f1',
          order:     '#0ea5e9',
          product:   '#f59e0b',
          delivery:  '#10b981',
          billing:   '#f97316',
          journal:   '#a78bfa',
          payment:   '#34d399',
        },
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4,0,0.6,1) infinite',
        'fade-in':    'fadeIn 0.4s ease forwards',
        'slide-up':   'slideUp 0.35s ease forwards',
        'stream':     'stream 0.6s ease forwards',
      },
      keyframes: {
        fadeIn:  { from: { opacity: '0' },              to: { opacity: '1' } },
        slideUp: { from: { opacity: '0', transform: 'translateY(12px)' }, to: { opacity: '1', transform: 'translateY(0)' } },
        stream:  { from: { opacity: '0', transform: 'translateY(6px)'  }, to: { opacity: '1', transform: 'translateY(0)' } },
      },
    },
  },
  plugins: [],
}
