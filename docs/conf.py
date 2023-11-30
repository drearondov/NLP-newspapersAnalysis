# -*- coding: utf-8 -*-
#
# Newspapers Analysis documentation build configuration file
import os
import sys

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
sys.path.insert(0, os.path.abspath(".."))

# -- General configuration -----------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
needs_sphinx = "7.2.3"

# Sphinx extension module names here, as strings.
extensions = ["sphinx.ext.autodoc", "sphinx.ext.napoleon"]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# The suffix of source filenames.
source_suffix = [".rst"]

# The encoding of source files.
# source_encoding = 'utf-8-sig'

# The master toctree document.
master_doc = "index"

# General information about the project.
project = "Newspapers Analysis"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|.

# The short X.Y version.
version = "0.1"
# The full version, including alpha/beta/rc tags.
release = "0.1.0"

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["_build"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "gruvbox-dark"

# Napoleon configurations
napoleon_google_docstring = True

# -- Options for HTML output ---------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
html_theme = "furo"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
# html_theme_options = {}

# The name for this set of Sphinx documents.  If None, it defaults to
# "<project> v<release> documentation".
html_title = "Newspapers Analysis Documentation"

# A shorter title for the navigation bar.  Default is the same as html_title.
# html_short_title = None

# The name of an image file (within the static path) to use as favicon of the
# docs.  This file should be a Windows icon file (.ico) being 16x16 or 32x32
# pixels large.
# html_favicon = None

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

# Output file base name for HTML help builder.
htmlhelp_basename = "newspapers_analysisdoc"


# -- Options for manual page output --------------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (
        "index",
        "newspapers_analysis",
        "Newspapers Analysis Documentation",
        ["Andrea Rondón"],
        1,
    )
]

# If true, show URL addresses after external links.
# man_show_urls = False
