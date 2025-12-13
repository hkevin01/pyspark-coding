# PySpark Coding Project - Code Style & Documentation Guide

## 📚 Purpose

This guide establishes consistent code documentation standards across the PySpark coding project. Following these guidelines ensures:
- **Readability**: Easy to understand for all skill levels
- **Maintainability**: Clear structure for future modifications
- **Educational Value**: Self-documenting code for learning
- **Team Efficiency**: Consistent patterns reduce onboarding time

---

## 🎯 Quick Reference

### File Structure Template

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
[Module Title]
================================================================================

MODULE OVERVIEW:
----------------
[1-2 sentence description]

PURPOSE:
--------
[Why this module exists]

TARGET AUDIENCE:
----------------
[Who should use this]

[SPECIFIC SECTIONS]:
====================
[Key topics covered]

KEY LEARNING OUTCOMES:
======================
[What users will learn]

USAGE:
------
[How to run/import]

RELATED RESOURCES:
------------------
[Links to docs/guides]

AUTHOR: PySpark Education Project
LICENSE: MIT License
VERSION: 1.0.0
CREATED: [Date]
LAST MODIFIED: [Date]

================================================================================
"""

# ============================================================================
# IMPORTS
# ============================================================================
# Standard library imports
import os
import sys

# Third-party imports
from pyspark.sql import SparkSession

# Local imports
from .utils import helper_function


# ============================================================================
# CONSTANTS
# ============================================================================
DEFAULT_APP_NAME = "MySparkApp"
MAX_RETRIES = 3


# ============================================================================
# MAIN FUNCTIONALITY
# ============================================================================

def main_function():
    """
    ============================================================================
    [✅ or ❌] [Function Title]
    ============================================================================
    
    WHAT THIS DEMONSTRATES:
    -----------------------
    [Clear explanation]
    
    THE PROBLEM/SOLUTION:
    --------------------
    [Detailed explanation]
    
    WHY IT FAILS/WORKS:
    ------------------
    [Step-by-step reasoning]
    
    ============================================================================
    """
    # ========================================================================
    # STEP 1: [First major step]
    # ========================================================================
    # Detailed comments explaining this step
    code_here()
    
    # ========================================================================
    # STEP 2: [Second major step]
    # ========================================================================
    # ✅ CORRECT: Explanation of safe pattern
    # ❌ DANGER: Explanation of dangerous pattern
    more_code()


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    main_function()
```

---

## 📝 Documentation Standards

### 1. Module-Level Documentation

**Required Sections (in order):**
1. Shebang line: `#!/usr/bin/env python3`
2. Encoding declaration: `# -*- coding: utf-8 -*-`
3. Module docstring with:
   - Title (80 `=` characters above and below)
   - MODULE OVERVIEW
   - PURPOSE
   - TARGET AUDIENCE
   - Content sections (varies by module)
   - KEY LEARNING OUTCOMES
   - USAGE
   - RELATED RESOURCES
   - Metadata (AUTHOR, LICENSE, VERSION, dates)

**Line Length:** 80 characters for documentation

### 2. Function Documentation

**Structure:**
```python
def function_name(param1, param2):
    """
    ============================================================================
    [✅ SAFE or ❌ DANGEROUS] [Short Descriptive Title]
    ============================================================================
    
    WHAT THIS DEMONSTRATES:
    -----------------------
    [1-3 sentences explaining what this function shows]
    
    THE PROBLEM/SOLUTION:
    --------------------
    [Detailed explanation of the issue or approach]
    
    WHY IT FAILS/WORKS:
    ------------------
    [Step-by-step breakdown of the mechanism]
    
    PARAMETERS:
    -----------
    param1 (type): Description
    param2 (type): Description
    
    RETURNS:
    --------
    type: Description of return value
    
    RAISES:
    -------
    ExceptionType: When and why
    
    EXAMPLES:
    ---------
    >>> function_name(1, 2)
    3
    
    REAL-WORLD SCENARIO:
    --------------------
    [Practical context where this applies]
    
    KEY PRINCIPLE:
    --------------
    [Core concept being illustrated]
    
    TRADE-OFFS:
    -----------
    ✅ Pros:
    - Benefit 1
    - Benefit 2
    
    ⚠️  Cons:
    - Limitation 1
    - Limitation 2
    
    BEST PRACTICES:
    ---------------
    1. Practice 1
    2. Practice 2
    
    SYMPTOMS (for dangerous patterns):
    -----------------------------------
    - Error message 1
    - Behavior 1
    
    SEE ALSO:
    ---------
    - related_function() - Description
    - Documentation: [URL]
    
    ============================================================================
    """
```

**Note:** Not all sections required for every function. Use judgment based on complexity and educational value.

### 3. Inline Comments

**Section Markers:**
```python
# ============================================================================
# MAJOR SECTION HEADER
# ============================================================================
```

**Sub-sections:**
```python
# ========================================================================
# STEP N: [Description of this step]
# ========================================================================
```

**Inline Patterns:**
```python
# ✅ CORRECT: This is the safe/recommended approach
safe_code()

# ❌ DANGER: This will cause problems because...
dangerous_code()

# ⚠️  WARNING: Be careful here because...
careful_code()

# 📝 NOTE: Additional context or explanation
informational_code()
```

---

## 🎨 Code Formatting

### Line Length
- **Code:** Max 88 characters (Black formatter)
- **Comments:** Max 80 characters
- **Docstrings:** Max 80 characters

### Import Organization
```python
# ============================================================================
# IMPORTS
# ============================================================================

# Standard library (alphabetical)
import os
import sys

# Third-party (alphabetical)
from pyspark.sql import SparkSession
import torch

# Local (relative)
from .utils import helper
```

### Naming Conventions
- **Variables/Functions:** `snake_case`
- **Classes:** `PascalCase`
- **Constants:** `UPPER_CASE`
- **Private:** `_leading_underscore`

### Spacing
- 2 blank lines before/after functions
- 1 blank line between logical blocks
- No blank lines inside functions except to separate major steps

---

## 🔍 Comment Best Practices

### DO:
✅ Explain WHY, not WHAT
✅ Use complete sentences with proper punctuation
✅ Keep comments up-to-date with code changes
✅ Add comments before complex logic
✅ Use section markers for visual hierarchy
✅ Include real-world context
✅ Cross-reference related code

### DON'T:
❌ State the obvious (e.g., `x = 5  # Set x to 5`)
❌ Leave outdated comments
❌ Use comments to explain bad code (refactor instead)
❌ Write novel-length comments (be concise)
❌ Use humor that may not age well

---

## 📊 Examples

### Good Documentation Example

```python
def calculate_partition_skew(df):
    """
    ============================================================================
    ✅ SAFE: Detect Data Skew Across Partitions
    ============================================================================
    
    WHAT THIS DEMONSTRATES:
    -----------------------
    Calculates partition size distribution to identify data skew issues
    before they cause executor OOM errors in production.
    
    THE SOLUTION:
    -------------
    Uses Spark's mapPartitionsWithIndex to count rows per partition,
    then calculates statistical measures (mean, stddev, max/min ratio)
    to quantify skew severity.
    
    WHY IT WORKS:
    -------------
    1. Each partition reports its row count locally
    2. Results collected to driver (small dataset)
    3. Statistics calculated to identify outliers
    4. Skew ratio > 3.0 indicates problematic skew
    
    RETURNS:
    --------
    dict: {
        'mean': float,           # Average partition size
        'stddev': float,         # Standard deviation
        'max_min_ratio': float,  # Ratio of largest to smallest
        'skew_score': float      # Overall skew severity (0-1)
    }
    
    BEST PRACTICES:
    ---------------
    1. Run this on sample data first (expensive on full dataset)
    2. Skew ratio > 3.0 requires salting or repartitioning
    3. Consider both row count and data size skew
    
    SEE ALSO:
    ---------
    - apply_salting_strategy() - Fixes detected skew
    - Spark Tuning Guide: https://spark.apache.org/docs/latest/tuning.html
    
    ============================================================================
    """
    # ========================================================================
    # STEP 1: Calculate partition sizes
    # ========================================================================
    # ✅ CORRECT: Use mapPartitionsWithIndex for efficient counting
    # Each partition counts its rows locally, avoiding shuffle
    def count_partition(index, iterator):
        count = sum(1 for _ in iterator)
        yield (index, count)
    
    partition_counts = (
        df.rdd
        .mapPartitionsWithIndex(count_partition)
        .collectAsMap()
    )
    
    # ========================================================================
    # STEP 2: Calculate skew statistics
    # ========================================================================
    counts = list(partition_counts.values())
    mean_size = sum(counts) / len(counts)
    max_size = max(counts)
    min_size = min(counts)
    
    # ⚠️  WARNING: Division by zero if min_size is 0 (empty partition)
    skew_ratio = max_size / max(min_size, 1)
    
    return {
        'mean': mean_size,
        'max_min_ratio': skew_ratio,
        'skew_score': min(skew_ratio / 10.0, 1.0)  # Normalized 0-1
    }
```

### Bad Documentation Example (Don't Do This)

```python
def calc_skew(df):
    """Calculates skew."""  # ❌ Too vague
    
    # get counts  # ❌ States obvious
    x = df.rdd.mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))]).collectAsMap()
    
    # do math  # ❌ Not helpful
    y = list(x.values())
    m = sum(y) / len(y)
    return m  # ❌ What does 'm' mean?
```

---

## ✅ Quality Checklist

Before committing code, verify:

### Module Level:
- [ ] Shebang and encoding present
- [ ] Complete module docstring (80+ lines for educational modules)
- [ ] All sections filled out appropriately
- [ ] Imports organized by category
- [ ] Constants defined before functions

### Function Level:
- [ ] Comprehensive docstring (20-60 lines for complex functions)
- [ ] WHY explained, not just WHAT
- [ ] Real-world context provided
- [ ] Trade-offs documented
- [ ] Related functions cross-referenced

### Code Level:
- [ ] Section markers for major blocks
- [ ] Step-by-step comments for complex logic
- [ ] Emoji indicators (✅/❌/⚠️) used appropriately
- [ ] No obvious comments (code should be self-explanatory)
- [ ] Comments updated with code changes

### Style:
- [ ] Line lengths under limits (88 code, 80 comments)
- [ ] Consistent naming conventions
- [ ] Proper spacing and indentation
- [ ] No trailing whitespace

---

## 🚀 Tools & Automation

### Recommended Tools:
- **Black**: Auto-format code
- **flake8**: Linting
- **pydocstyle**: Docstring validation
- **mypy**: Type checking
- **pre-commit**: Automated checks

### Pre-commit Hook Example:
```yaml
repos:
  - repo: https://github.com/psf/black
    rev: 23.3.0
    hooks:
      - id: black
        language_version: python3.10
  
  - repo: https://github.com/PyCQA/flake8
    rev: 6.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=88]
```

---

## 📖 References

- **PEP 8**: Python Style Guide
- **PEP 257**: Docstring Conventions
- **Google Python Style Guide**
- **Black Code Style**
- **NumPy Documentation Style**

---

## 🔄 Version History

- **v1.0.0** (2025): Initial style guide established
  - Module header standards
  - Function documentation structure
  - Inline comment patterns
  - Quality checklist

---

**Maintained by:** PySpark Education Project Team  
**Last Updated:** December 2025  
**License:** MIT
