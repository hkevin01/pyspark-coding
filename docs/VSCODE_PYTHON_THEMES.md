# VS Code Python Color Themes Guide

## Current Configuration ✅

**Active Theme**: One Dark Pro  
**Import Colors**: Blue (#61AFEF) and Purple (#C678DD) - NO RED!  
**Error Display**: Minimized (transparent red underlines)

## Most Popular Python Themes (by downloads)

### 1. **One Dark Pro** ⭐ CURRENT
- **Downloads**: 10M+
- **Import Color**: Blue/Cyan
- **Best For**: Python, JavaScript, TypeScript
- **Install**: Already installed!
```
Theme ID: "One Dark Pro"
```

### 2. **Material Theme**
- **Downloads**: 5M+
- **Import Color**: Purple/Blue (varies by variant)
- **Variants**: Ocean, Palenight, Darker, Lighter
- **Install**: `code --install-extension Equinusocio.vsc-material-theme`
```
Theme ID: "Material Theme Ocean" (recommended for Python)
```

### 3. **Dracula Official**
- **Downloads**: 3M+
- **Import Color**: Pink/Purple
- **Best For**: Low-light coding
- **Install**: `code --install-extension dracula-theme.theme-dracula`
```
Theme ID: "Dracula"
```

### 4. **Nord**
- **Downloads**: 2M+
- **Import Color**: Cyan/Blue
- **Best For**: Minimal, Arctic aesthetic
- **Install**: `code --install-extension arcticicestudio.nord-visual-studio-code`
```
Theme ID: "Nord"
```

### 5. **Monokai Pro**
- **Downloads**: 1M+
- **Import Color**: Blue/Green
- **Best For**: Classic Monokai fans
- **Install**: `code --install-extension monokai.theme-monokai-pro-vscode`
```
Theme ID: "Monokai Pro"
```

## Quick Theme Switching

### Method 1: Command Palette
1. Press `Ctrl+Shift+P`
2. Type "Color Theme"
3. Select "Preferences: Color Theme"
4. Choose your theme

### Method 2: Settings File
Edit `~/.config/Code/User/settings.json`:
```json
{
  "workbench.colorTheme": "One Dark Pro"
}
```

### Method 3: Terminal Command
```bash
# Update settings.json programmatically
python3 -c "
import json
path = '~/.config/Code/User/settings.json'
with open(path.expanduser(path)) as f:
    s = json.load(f)
s['workbench.colorTheme'] = 'Dracula'
with open(path.expanduser(path), 'w') as f:
    json.dump(s, f, indent=2)
"
```

## Custom Import Colors (Already Applied)

Your current settings include custom colors to prevent red imports:

```json
{
  "editor.tokenColorCustomizations": {
    "textMateRules": [
      {
        "scope": "keyword.control.import.python",
        "settings": {
          "foreground": "#61AFEF"  // Blue
        }
      },
      {
        "scope": "keyword.control.from.python",
        "settings": {
          "foreground": "#C678DD"  // Purple
        }
      }
    ]
  }
}
```

## Disable Error/Warning Colors

Already applied to minimize red squiggles:

```json
{
  "workbench.colorCustomizations": {
    "editorError.foreground": "#ff000000",     // Transparent
    "editorWarning.foreground": "#ffcc0066"    // Semi-transparent
  }
}
```

## Install All Popular Themes at Once

```bash
# Install all 5 most popular themes
code --install-extension zhuangtongfa.Material-theme
code --install-extension dracula-theme.theme-dracula
code --install-extension arcticicestudio.nord-visual-studio-code
code --install-extension monokai.theme-monokai-pro-vscode
code --install-extension Equinusocio.vsc-material-theme
```

## Recommended Settings for Python

```json
{
  // Theme
  "workbench.colorTheme": "One Dark Pro",
  
  // Minimize errors/warnings
  "python.analysis.diagnosticMode": "off",
  "python.linting.enabled": false,
  
  // Custom colors (no red imports)
  "editor.tokenColorCustomizations": {
    "textMateRules": [
      {
        "scope": "keyword.control.import.python",
        "settings": { "foreground": "#61AFEF" }
      }
    ]
  },
  
  // Reduce error visibility
  "workbench.colorCustomizations": {
    "editorError.foreground": "#ff000000",
    "editorWarning.foreground": "#ffcc0066"
  }
}
```

## Theme Comparison for Python

| Theme | Import Color | Background | Popularity | Python Rating |
|-------|-------------|------------|------------|---------------|
| **One Dark Pro** | Blue/Cyan | Dark | ⭐⭐⭐⭐⭐ | Excellent |
| Material Ocean | Purple/Blue | Dark Blue | ⭐⭐⭐⭐⭐ | Excellent |
| Dracula | Pink/Purple | Very Dark | ⭐⭐⭐⭐ | Good |
| Nord | Cyan/Blue | Arctic Blue | ⭐⭐⭐⭐ | Excellent |
| Monokai Pro | Blue/Green | Dark | ⭐⭐⭐⭐ | Good |

## Current Status

✅ **One Dark Pro** installed and active  
✅ Custom import colors (Blue/Purple, not red)  
✅ Error squiggles minimized  
✅ All required Python packages installed:
- PySpark 4.0.1
- PyTorch 2.9.1
- Pandas, NumPy, Scikit-learn
- Matplotlib, Seaborn, PyArrow

## Reload VS Code

After theme changes, reload VS Code:
- **Command Palette**: `Ctrl+Shift+P` → "Developer: Reload Window"
- **Or**: Close and reopen VS Code

---

**Note**: Your settings have been updated. Restart VS Code to see the new theme with blue/purple import colors instead of red!
