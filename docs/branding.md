# EngageQueue branding

The EngageQueue logo is a diagonal pointing hand with a short red-and-black
captain-style cuff. A black outline defines the hand and sleeve against a
transparent background.

<img src="assets/engagequeue-logo.svg" alt="EngageQueue pointing-hand logo" width="320">

Download the [editable SVG](assets/engagequeue-logo.svg) or the matching
[transparent 1024 × 1024 PNG](assets/engagequeue-logo.png).

## Using the logo

Use the SVG for scalable display and editing, or the PNG where raster images are
required. Preserve the logo's proportions and full square canvas. Its crop fits
both square icons and centered circular avatars; keep the transparent margins
so the fingertip and cuff remain inside a circular crop.

## Updating the assets

The SVG is the authoritative editable source. Edit it first, then export the PNG
so both formats show the same artwork and crop.

With the librsvg command-line tools installed, regenerate the PNG from the
repository root:

```sh
rsvg-convert --width 1024 --height 1024 docs/assets/engagequeue-logo.svg --output docs/assets/engagequeue-logo.png
```
