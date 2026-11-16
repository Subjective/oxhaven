//! Colours and the styles a table format packs into a `long`.
//!
//! A format of a table is a hidden column of `long` styles: the background in the upper
//! half, the foreground in the lower, each with a byte of flags above its red, green and
//! blue. Java computes one with the static methods of `ColorUtilImpl`, which take a
//! `Color`, the name of one, or a style.
//!
//! A formula here has no value of type `Color`. Every method of `ColorUtilImpl` that takes
//! a `Color` first converts it with `toLong(Color)`, so the type checker
//! (`types::color`) carries a colour as that `long`, and the functions below take styles
//! only. The value of a formula cannot be a `Color`, and its methods cannot be called.
//!
//! The methods are Java methods of primitives: they do not look for null, and compute
//! with the value Java reserves for it. So do these, and a result that is that value is
//! null.

use super::color_table::{FIELDS, NAMES};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int64Array};
use arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
use oxhaven_contracts::table::TableFailure;
use std::sync::Arc;

const COLOR_SET_BIT: i64 = 0x01;
const SELECTION_OVERRIDE_BIT: i64 = 0x02;
const FOREGROUND_SET_BIT: i64 = 0x04;
const UPPER: i64 = 0xffff_ffff_0000_0000_u64 as i64;
const NULL_DOUBLE: f64 = -f64::MAX;

/// A function of colours and styles, on `long` styles.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ColorFn {
    /// `toLong(long)`: zero for null.
    ToLong,
    /// `toLong(String)`: the style of the colour of a name, zero for null and for
    /// `NO_FORMATTING`.
    OfName,
    /// `toLong(color(String))`: as `OfName`, but null fails and `NO_FORMATTING` is black.
    ColorOfName,
    /// `toLong(colorRGB(int, int, int))` and `(int, int, int, int)`.
    Rgb,
    /// `toLong(colorRGB(int))`.
    RgbPacked,
    /// `toLong(colorRGB(float, float, float))` and `(float, float, float, float)`.
    RgbFloat,
    /// `toLong(colorHSL(float, float, float))` and `(float, float, float, float)`.
    Hsl,
    /// `bg(long)`.
    Background,
    /// `bg(long, long, long)`.
    BackgroundRgb,
    /// `bgo(long)`.
    BackgroundOverride,
    /// `fg(long)`.
    Foreground,
    /// `fg(long, long, long)`.
    ForegroundRgb,
    /// `fgo(long)`.
    ForegroundOverride,
    /// `bgfg(long, long)`.
    BackgroundForeground,
    /// `bgfg(long, long, long, long, long, long)`.
    BackgroundForegroundRgb,
    /// `bgfga(long)`.
    Auto,
    /// `bgfga(long, long, long)`.
    AutoRgb,
    /// `heatmap(double, double, double, long, long)`.
    Heatmap,
    /// `heatmapFg(double, double, double, long, long)`.
    HeatmapForeground,
    /// `isBackgroundSelectionOverride(long)`.
    IsBackgroundOverride,
    /// `isForegroundSelectionOverride(long)`.
    IsForegroundOverride,
}

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

fn background_rgb(r: i64, g: i64, b: i64) -> i64 {
    (COLOR_SET_BIT << 56) | r.wrapping_shl(48) | g.wrapping_shl(40) | b.wrapping_shl(32)
}

fn background(style: i64) -> i64 {
    style & UPPER
}

fn foreground_rgb(r: i64, g: i64, b: i64) -> i64 {
    (COLOR_SET_BIT << 24) | r.wrapping_shl(16) | g.wrapping_shl(8) | b | (FOREGROUND_SET_BIT << 24)
}

fn foreground(style: i64) -> i64 {
    (((style & UPPER) as u64) >> 32) as i64 | (FOREGROUND_SET_BIT << 24)
}

fn yiq(r: i64, g: i64, b: i64) -> i64 {
    (r.wrapping_mul(299)
        .wrapping_add(g.wrapping_mul(587))
        .wrapping_add(b.wrapping_mul(114)))
        / 1000
}

fn auto_foreground(r: i64, g: i64, b: i64) -> i64 {
    if yiq(r, g, b) >= 128 {
        0x0100_0000
    } else {
        0x01e0_e0e0
    }
}

/// Behavior: `ColorUtilImpl#backgroundForegroundAuto(long, long, long)`.
fn auto_rgb(r: i64, g: i64, b: i64) -> i64 {
    background_rgb(r, g, b) | auto_foreground(r, g, b)
}

/// Behavior: `ColorUtilImpl#backgroundForegroundAuto(long)`, which reads the red, green and
/// blue of the background for the brightness.
fn auto(style: i64) -> i64 {
    let channel = |shift: u32| (style >> shift) & 0xff;
    background(style) | auto_foreground(channel(48), channel(40), channel(32))
}

/// The style of a colour: `toLong(Color)` of a colour that is not `NO_FORMATTING`.
pub(crate) fn style_of(rgb: [u8; 3]) -> i64 {
    auto_rgb(rgb[0].into(), rgb[1].into(), rgb[2].into())
}

/// `toLong` of the field `name` of `Color`, the colour a formula names as `name`.
///
/// Behavior: `ColorUtilImpl#toLong(Color)`: zero for `Color.NO_FORMATTING` itself, which
/// is not the black other fields are.
pub(crate) fn field(name: &str) -> Option<i64> {
    let index = FIELDS.binary_search_by(|(field, _)| field.cmp(&name)).ok()?;
    Some(if name == "NO_FORMATTING" {
        0
    } else {
        style_of(FIELDS[index].1)
    })
}

/// The colour `Color#Color(String)` makes of `name`.
///
/// Behavior: a name with a `#` is read by `java.awt.Color#decode`, which is
/// `Integer#decode` and keeps the lower three bytes; any other is a constant of `Colors`,
/// whatever its case.
fn named(name: &str) -> Result<[u8; 3], TableFailure> {
    if name.contains('#') {
        let (negative, digits) = match name.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, name.strip_prefix('+').unwrap_or(name)),
        };
        let value = digits
            .strip_prefix('#')
            .filter(|hex| !hex.starts_with(['+', '-']))
            .and_then(|hex| i64::from_str_radix(hex, 16).ok())
            .map(|value| if negative { -value } else { value })
            .filter(|value| i32::try_from(*value).is_ok())
            .ok_or_else(|| {
                failure(format!(
                    "java.lang.NumberFormatException: For input string: \"{name}\""
                ))
            })?;
        return Ok([(value >> 16) as u8, (value >> 8) as u8, value as u8]);
    }
    let upper = name.to_uppercase();
    NAMES
        .binary_search_by(|(constant, _)| (*constant).cmp(upper.as_str()))
        .map(|index| NAMES[index].1)
        .map_err(|_| {
            failure(format!(
                "java.lang.IllegalArgumentException: Invalid color name: {name}"
            ))
        })
}

/// Behavior: `ColorUtilImpl#toLong(String)`.
fn of_name(name: Option<&str>) -> Result<i64, TableFailure> {
    match name {
        None => Ok(0),
        Some(name) if name.to_uppercase() == "NO_FORMATTING" => Ok(0),
        Some(name) => named(name).map(style_of),
    }
}

/// Behavior: `java.awt.Color#Color(int, int, int, int)`, which refuses a part outside 0 to
/// 255.
fn rgb(parts: &[i32]) -> Result<i64, TableFailure> {
    if parts.iter().any(|part| !(0..=255).contains(part)) {
        return Err(failure(
            "java.lang.IllegalArgumentException: Color parameter outside of expected range",
        ));
    }
    Ok(auto_rgb(parts[0].into(), parts[1].into(), parts[2].into()))
}

/// Behavior: `java.awt.Color#Color(float, float, float, float)`: each part between 0 and
/// 1, and the byte nearest to 255 times it.
fn rgb_float(parts: &[f32]) -> Result<i64, TableFailure> {
    // As Java tests it: a part that is not a number passes, and is the byte 0.
    if parts.iter().any(|part| *part < 0.0 || *part > 1.0) {
        return Err(failure(
            "java.lang.IllegalArgumentException: Color parameter outside of expected range",
        ));
    }
    let byte = |part: f32| i64::from((part * 255.0 + 0.5) as i32);
    Ok(auto_rgb(byte(parts[0]), byte(parts[1]), byte(parts[2])))
}

/// Behavior: `Color#hslToColor`, in `float` as Java computes it.
fn hsl(parts: &[f32]) -> Result<i64, TableFailure> {
    let (h, s, l) = (parts[0], parts[1], parts[2]);
    let alpha = parts.get(3).copied().unwrap_or(1.0);
    let outside = |what: &str| {
        failure(format!(
            "java.lang.IllegalArgumentException: Color parameter outside of expected range - {what}"
        ))
    };
    if !(0.0..=100.0).contains(&s) && !s.is_nan() {
        return Err(outside("Saturation"));
    }
    if !(0.0..=100.0).contains(&l) && !l.is_nan() {
        return Err(outside("Luminance"));
    }
    if !(0.0..=1.0).contains(&alpha) && !alpha.is_nan() {
        return Err(outside("Alpha"));
    }
    let h = (h % 360.0) / 360.0;
    let s = s / 100.0;
    let l = l / 100.0;
    let q = if f64::from(l) < 0.5 {
        l * (1.0 + s)
    } else {
        (l + s) - (s * l)
    };
    let p = 2.0 * l - q;
    let channel = |mut h: f32| {
        if h < 0.0 {
            h += 1.0;
        }
        if h > 1.0 {
            h -= 1.0;
        }
        let value = if 6.0 * h < 1.0 {
            p + ((q - p) * 6.0 * h)
        } else if 2.0 * h < 1.0 {
            q
        } else if 3.0 * h < 2.0 {
            p + ((q - p) * 6.0 * ((2.0 / 3.0) - h))
        } else {
            p
        };
        // `Math.max(0, x)` and `Math.min(x, 1)`: NaN stays NaN and is refused below.
        if value.is_nan() {
            value
        } else {
            value.clamp(0.0, 1.0)
        }
    };
    rgb_float(&[
        channel(h + (1.0 / 3.0)),
        channel(h),
        channel(h - (1.0 / 3.0)),
        alpha,
    ])
}

/// Behavior: `ColorUtilImpl#heatmap(double, double, double, long, long)` and
/// `#heatmapForeground` of the same parameters.
fn heatmap(value: f64, min: f64, max: f64, low: i64, high: i64, foreground: bool) -> i64 {
    if value <= min {
        return low;
    }
    if value >= max {
        return high;
    }
    let part = (value - min) / (max - min);
    let shifts: [u32; 3] = if foreground {
        [16, 8, 0]
    } else {
        [48, 40, 32]
    };
    let mix = |shift: u32| {
        let from = ((low >> shift) & 0xff) as f64;
        let to = ((high >> shift) & 0xff) as f64;
        // A Java cast of `double` to `long`: NaN is zero, which `as` gives too.
        (from + part * (to - from)) as i64
    };
    let (r, g, b) = (mix(shifts[0]), mix(shifts[1]), mix(shifts[2]));
    if foreground {
        foreground_rgb(r, g, b)
    } else {
        auto_rgb(r, g, b)
    }
}

/// Evaluates `function` over arguments of its parameter types: `long` for a style or a
/// part of one, `double` for the first three of a heat map, `String` for a name, `int`
/// or `float` for the parts of a colour.
pub(crate) fn evaluate(function: ColorFn, arguments: &[ArrayRef]) -> Result<ArrayRef, TableFailure> {
    let rows = arguments[0].len();
    let long = |index: usize, row: usize| {
        let values = arguments[index].as_primitive::<Int64Type>();
        if values.is_null(row) {
            i64::MIN
        } else {
            values.value(row)
        }
    };
    let double = |index: usize, row: usize| {
        let values = arguments[index].as_primitive::<Float64Type>();
        if values.is_null(row) {
            NULL_DOUBLE
        } else {
            values.value(row)
        }
    };
    let ints = |row: usize| -> Vec<i32> {
        arguments
            .iter()
            .map(|argument| {
                let values = argument.as_primitive::<Int32Type>();
                if values.is_null(row) {
                    i32::MIN
                } else {
                    values.value(row)
                }
            })
            .collect()
    };
    let floats = |row: usize| -> Vec<f32> {
        arguments
            .iter()
            .map(|argument| {
                let values = argument.as_primitive::<Float32Type>();
                if values.is_null(row) {
                    -f32::MAX
                } else {
                    values.value(row)
                }
            })
            .collect()
    };
    let text = |row: usize| {
        let values = arguments[0].as_string::<i32>();
        (!values.is_null(row)).then(|| values.value(row))
    };
    let test = |bit: i64| -> ArrayRef {
        Arc::new(BooleanArray::from_iter(
            (0..rows).map(|row| Some(long(0, row) & bit != 0)),
        ))
    };
    let style = |row: usize| -> Result<i64, TableFailure> {
        Ok(match function {
            ColorFn::ToLong => match long(0, row) {
                i64::MIN => 0,
                style => style,
            },
            ColorFn::OfName => of_name(text(row))?,
            ColorFn::ColorOfName => match text(row) {
                None => {
                    return Err(failure(
                        "java.lang.IllegalArgumentException: Color can not be null",
                    ))
                }
                Some(name) => style_of(named(name)?),
            },
            ColorFn::Rgb => rgb(&ints(row))?,
            ColorFn::RgbPacked => {
                let packed = i64::from(ints(row)[0]);
                auto_rgb((packed >> 16) & 0xff, (packed >> 8) & 0xff, packed & 0xff)
            }
            ColorFn::RgbFloat => rgb_float(&floats(row))?,
            ColorFn::Hsl => hsl(&floats(row))?,
            ColorFn::Background => background(long(0, row)),
            ColorFn::BackgroundRgb => background_rgb(long(0, row), long(1, row), long(2, row)),
            ColorFn::BackgroundOverride => {
                background(long(0, row)) | (SELECTION_OVERRIDE_BIT << 56)
            }
            ColorFn::Foreground => foreground(long(0, row)),
            ColorFn::ForegroundRgb => foreground_rgb(long(0, row), long(1, row), long(2, row)),
            ColorFn::ForegroundOverride => {
                foreground(long(0, row)) | (SELECTION_OVERRIDE_BIT << 24)
            }
            ColorFn::BackgroundForeground => {
                let fg = long(1, row);
                let fg = if fg & (FOREGROUND_SET_BIT << 24) != 0 {
                    fg
                } else {
                    foreground(fg)
                };
                background(long(0, row)) | fg
            }
            ColorFn::BackgroundForegroundRgb => {
                background_rgb(long(0, row), long(1, row), long(2, row))
                    | foreground_rgb(long(3, row), long(4, row), long(5, row))
            }
            ColorFn::Auto => auto(long(0, row)),
            ColorFn::AutoRgb => auto_rgb(long(0, row), long(1, row), long(2, row)),
            ColorFn::Heatmap | ColorFn::HeatmapForeground => heatmap(
                double(0, row),
                double(1, row),
                double(2, row),
                long(3, row),
                long(4, row),
                function == ColorFn::HeatmapForeground,
            ),
            ColorFn::IsBackgroundOverride | ColorFn::IsForegroundOverride => {
                unreachable!("a test, not a style")
            }
        })
    };
    match function {
        ColorFn::IsBackgroundOverride => Ok(test(SELECTION_OVERRIDE_BIT << 56)),
        ColorFn::IsForegroundOverride => Ok(test(SELECTION_OVERRIDE_BIT << 24)),
        _ => {
            let styles = (0..rows)
                .map(|row| style(row).map(|style| (style != i64::MIN).then_some(style)))
                .collect::<Result<Int64Array, TableFailure>>()?;
            Ok(Arc::new(styles))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The values are those Deephaven 42.9 sent for the same formulas.
    #[test]
    fn styles_of_colours_are_javas() {
        assert_eq!(field("RED"), Some(143833713130660064));
        assert_eq!(field("NO_FORMATTING"), Some(0));
        assert_eq!(field("BLACK"), Some(72057594069442784));
        assert_eq!(field("DB_PINK"), Some(144026879270060032));
        assert_eq!(field("Red"), None);
        assert_eq!(of_name(Some("green")).unwrap(), 72198331557798112);
        assert_eq!(of_name(Some("No_Formatting")).unwrap(), 0);
        assert_eq!(of_name(None).unwrap(), 0);
        assert_eq!(of_name(Some("#F0A")).unwrap(), 72074129693532384);
        assert_eq!(of_name(Some("#FF00AA")).unwrap(), 143834443275100384);
        assert!(of_name(Some("db_pink")).is_err());
        assert!(of_name(Some(" red")).is_err());
        assert_eq!(style_of(named("NO_FORMATTING").unwrap()), 72057594069442784);
    }

    #[test]
    fn colours_of_parts_are_javas() {
        assert_eq!(hsl(&[120.0, 50.0, 50.0]).unwrap(), 90282274162999296);
        assert_eq!(hsl(&[60.0, 100.0, 50.0]).unwrap(), 144114088581005312);
        assert_eq!(rgb_float(&[0.5, 0.25, 1.0]).unwrap(), 108157855049244896);
        assert_eq!(rgb(&[40, 0, 0]).unwrap(), 83316593137869024);
        assert!(rgb(&[300, 0, 0]).is_err());
    }

    #[test]
    fn heat_maps_mix_the_parts() {
        let (red, blue) = (field("RED").unwrap(), field("BLUE").unwrap());
        assert_eq!(heatmap(1.5, 1.0, 4.0, red, blue, false), 131730469520728288);
        assert_eq!(heatmap(NULL_DOUBLE, -3e9, 5.0, red, blue, false), red);
        assert_eq!(
            heatmap(1.5, 1.0, 4.0, foreground(red), foreground(blue), true),
            97779754
        );
        assert_eq!(foreground(i64::MIN), 2214592512);
        assert_eq!(background(i64::MIN), i64::MIN);
    }
}
