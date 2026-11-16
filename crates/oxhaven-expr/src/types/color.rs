//! Colours and styles in a formula: the calls of `ColorUtilImpl` and of `Color`.
//!
//! A value of type `Color` is carried as the `long` that `ColorUtilImpl#toLong(Color)`
//! makes of it (`functions::color`), and exists only as an argument of a function that
//! takes a `Color`: [`Checker::color`] says whether an expression is one.

use super::{constant, convert, is_null_literal, type_error, typed, unsupported};
use super::{Checker, Node, Numeric, Typed};
use crate::functions::color::{self, ColorFn};
use crate::functions::Kernel;
use oxhaven_contracts::expr::{Expr, FormulaError, Literal};
use oxhaven_contracts::schema::ColumnType;

const COLOR_CLASS: &str = "io.deephaven.gui.color.Color";

/// The functions of `ColorUtilImpl` that are evaluated here. `valueOf` is not: Java does
/// not compile a call of it, since `Color` has a `valueOf` too.
const STYLE_FUNCTIONS: [&str; 17] = [
    "background",
    "backgroundForeground",
    "backgroundForegroundAuto",
    "backgroundOverride",
    "bg",
    "bgfg",
    "bgfga",
    "bgo",
    "fg",
    "fgo",
    "foreground",
    "foregroundOverride",
    "heatmap",
    "heatmapFg",
    "heatmapForeground",
    "isBackgroundSelectionOverride",
    "isForegroundSelectionOverride",
];

/// An argument of a function of styles.
enum Argument {
    /// A `Color`, as its style.
    Color(Typed),
    /// A `String`.
    Name(Typed),
    /// A number, of any primitive type.
    Number(Typed),
    Other,
    Null,
}

fn style(function: ColorFn, arguments: Vec<Typed>) -> Typed {
    typed(
        ColumnType::Long,
        Node::Call {
            kernel: Kernel::Color(function),
            arguments,
        },
    )
}

fn is_integral(value: &Typed) -> bool {
    matches!(
        value.column_type,
        ColumnType::Byte
            | ColumnType::Short
            | ColumnType::Char
            | ColumnType::Int
            | ColumnType::Long
    )
}

fn is_int(value: &Typed) -> bool {
    is_integral(value) && value.column_type != ColumnType::Long
}

fn refused_value() -> FormulaError {
    unsupported(
        "a value of type Color outside an argument of a function of colours \
         (io.deephaven.gui.color.Color)",
    )
}

/// Whether `name` is a function of `Color` that makes one.
pub(super) fn makes_color(name: &str) -> bool {
    matches!(name, "color" | "colorRGB" | "colorHSL")
}

/// The class whose fields are the styles of colours, as `long`s.
const COLOR_UTIL_CLASS: &str = "io.deephaven.engine.util.ColorUtil";
const COLOR_UTIL_IMPL_CLASS: &str = "io.deephaven.engine.util.ColorUtilImpl";

/// The prefixes and hues of the colours of `ColorUtil` other than the plain ones.
const SHADES: [&str; 12] = [
    "VIVID_",
    "STRONG_",
    "BRIGHT_",
    "PALE_",
    "VERYPALE_",
    "LIGHTGRAYISH_",
    "LIGHT_",
    "GRAYISH_",
    "DULL_",
    "DEEP_",
    "DARK_",
    "DARKGRAYISH_",
];
const HUES: [&str; 10] = [
    "RED",
    "YELLOWRED",
    "YELLOW",
    "GREENYELLOW",
    "GREEN",
    "BLUEGREEN",
    "BLUE",
    "PURPLEBLUE",
    "PURPLE",
    "REDPURPLE",
];

/// The value of the field `field` of the class `class` (named by its full name, as
/// neither class is imported by its simple name) when it is a constant of colours: the
/// three bits of `ColorUtilImpl`, which `ColorUtil` extends, and the 139 colours of
/// `ColorUtil`, each the style of the field of `Color` of its name, but `PINK`, `ORANGE`
/// and `GREEN`, which are those of `DB_PINK`, `DB_ORANGE` and `DB_GREEN`.
///
/// Behavior: the `public static final long` fields of io.deephaven.engine.util.ColorUtil
/// and io.deephaven.engine.util.ColorUtilImpl
pub(super) fn color_util_field(class: &str, field: &str) -> Option<(ColumnType, Literal)> {
    let long = |value: i64| Some((ColumnType::Long, Literal::Long(value)));
    if class != COLOR_UTIL_CLASS && class != COLOR_UTIL_IMPL_CLASS {
        return None;
    }
    match field {
        "COLOR_SET_BIT" => return long(0x01),
        "SELECTION_OVERRIDE_BIT" => return long(0x02),
        "FOREGROUND_SET_BIT" => return long(0x04),
        _ if class == COLOR_UTIL_IMPL_CLASS => return None,
        _ => {}
    }
    let shaded = SHADES.iter().any(|shade| {
        field
            .strip_prefix(shade)
            .is_some_and(|hue| HUES.contains(&hue))
    });
    let plain = matches!(
        field,
        "BLACK"
            | "GRAY1"
            | "GRAY2"
            | "GRAY3"
            | "GRAY4"
            | "GRAY5"
            | "GRAY6"
            | "GRAY7"
            | "GRAY8"
            | "WHITE"
            | "RED"
            | "PINK"
            | "ORANGE"
            | "YELLOW"
            | "GREEN"
            | "MAGENTA"
            | "CYAN"
            | "BLUE"
            | "NO_FORMATTING"
    );
    if !shaded && !plain {
        return None;
    }
    let color = match field {
        "PINK" => "DB_PINK",
        "ORANGE" => "DB_ORANGE",
        "GREEN" => "DB_GREEN",
        other => other,
    };
    color::field(color).and_then(long)
}

impl Checker<'_> {
    /// The colour a name is: a field of `Color`, unless a column has the name.
    ///
    /// Behavior: `QueryLanguageParser#visit(NameExpr)`, where a column comes before a
    /// static import, and `QueryLibraryImportsDefaults#statics`, which imports the fields
    /// of `Color`. The class is named by its full name only.
    fn color_field(&self, name: &str) -> Option<i64> {
        let field = match name.rsplit_once('.') {
            Some((COLOR_CLASS, field)) => field,
            Some(_) => return None,
            None if self.definition.index_of(name).is_some() => return None,
            None => name,
        };
        color::field(field)
    }

    /// Refuses a name that is a `Color` where a value of a column type is needed.
    pub(super) fn refuse_color_name(&self, name: &str) -> Result<(), FormulaError> {
        match self.color_field(name) {
            Some(_) => Err(refused_value()),
            None => Ok(()),
        }
    }

    /// `expr` as the style of the `Color` it is, or `None` when it is no `Color`.
    ///
    /// A `Color` is a field of `Color`, a call of `color`, `colorRGB` or `colorHSL`, and a
    /// `?:` of two of them or of one and `null`, which `toLong(Color)` makes zero.
    pub(super) fn color(&mut self, expr: &Expr) -> Result<Option<Typed>, FormulaError> {
        match expr {
            Expr::Column(name) => Ok(self
                .color_field(name)
                .map(|style| constant(ColumnType::Long, Literal::Long(style)))),
            Expr::Call {
                function,
                arguments,
            } if makes_color(function) => self.make_color(function, arguments).map(Some),
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } if makes_color(method)
                && matches!(&**receiver, Expr::Column(class) if class == COLOR_CLASS) =>
            {
                self.make_color(method, arguments).map(Some)
            }
            Expr::Ternary {
                condition,
                if_true,
                if_false,
            } => {
                let none = || constant(ColumnType::Long, Literal::Long(0));
                let (if_true, if_false) =
                    match (is_null_literal(if_true), is_null_literal(if_false)) {
                        (true, true) => return Ok(None),
                        (true, false) => (Some(none()), self.color(if_false)?),
                        (false, true) => (self.color(if_true)?, Some(none())),
                        (false, false) => {
                            let Some(if_true) = self.color(if_true)? else {
                                return Ok(None);
                            };
                            (Some(if_true), self.color(if_false)?)
                        }
                    };
                let (Some(if_true), Some(if_false)) = (if_true, if_false) else {
                    return Ok(None);
                };
                let condition = self.check(condition)?;
                if condition.column_type != ColumnType::Boolean {
                    return Err(type_error("the condition of ?: is not boolean"));
                }
                Ok(Some(typed(
                    ColumnType::Long,
                    Node::Ternary {
                        condition: Box::new(condition),
                        if_true: Box::new(if_true),
                        if_false: Box::new(if_false),
                    },
                )))
            }
            _ => Ok(None),
        }
    }

    /// The style of `color(..)`, `colorRGB(..)` or `colorHSL(..)`.
    ///
    /// Behavior: the overloads of `Color#color`, `#colorRGB` and `#colorHSL`, chosen as
    /// Java chooses: the `int` ones for arguments that widen to `int`, the `float` ones
    /// for the others that widen to `float`.
    fn make_color(&mut self, function: &str, arguments: &[Expr]) -> Result<Typed, FormulaError> {
        let no_overload = || type_error(format!("cannot find {function}() of these arguments"));
        let mut checked = Vec::with_capacity(arguments.len());
        for argument in arguments {
            if is_null_literal(argument) {
                return Err(unsupported(format!(
                    "null literal as an argument of {function}()"
                )));
            }
            checked.push(self.check(argument)?);
        }
        let all_to = |checked: Vec<Typed>, to: ColumnType| -> Vec<Typed> {
            checked
                .into_iter()
                .map(|argument| convert(argument, &to))
                .collect()
        };
        let ints = checked.iter().all(is_int);
        let floats = checked.iter().all(|argument| {
            Numeric::of(&argument.column_type).is_some()
                && argument.column_type != ColumnType::Double
        });
        match (function, checked.len()) {
            ("color", 1) if checked[0].column_type == ColumnType::String => {
                Ok(style(ColorFn::ColorOfName, checked))
            }
            ("colorRGB", 1) if ints => {
                Ok(style(ColorFn::RgbPacked, all_to(checked, ColumnType::Int)))
            }
            ("colorRGB", 3 | 4) if ints => {
                Ok(style(ColorFn::Rgb, all_to(checked, ColumnType::Int)))
            }
            ("colorRGB", 3 | 4) if floats => {
                Ok(style(ColorFn::RgbFloat, all_to(checked, ColumnType::Float)))
            }
            ("colorHSL", 3 | 4) if floats => {
                Ok(style(ColorFn::Hsl, all_to(checked, ColumnType::Float)))
            }
            // `colorRGB(int, boolean)` keeps the alpha when the flag says so, which no style
            // has: `ColorUtilImpl#toLong(Color)` and the functions of styles read the red,
            // green and blue of a colour only, so the style is that of `colorRGB(int)`
            // (observed on Java 42.9). A flag that may be null, which Java unboxes, is not
            // ported.
            // Behavior: Color#colorRGB(int, boolean), java.awt.Color#Color(int, boolean)
            ("colorRGB", 2) if is_int(&checked[0]) => match &arguments[1] {
                Expr::Literal(Literal::Boolean(_)) => {
                    let rgb = checked.swap_remove(0);
                    Ok(style(
                        ColorFn::RgbPacked,
                        all_to(vec![rgb], ColumnType::Int),
                    ))
                }
                _ if checked[1].column_type == ColumnType::Boolean => Err(unsupported(
                    "function colorRGB(int, boolean) of a flag that is not a literal",
                )),
                _ => Err(no_overload()),
            },
            _ => Err(no_overload()),
        }
    }

    fn style_argument(&mut self, expr: &Expr) -> Result<Argument, FormulaError> {
        if is_null_literal(expr) {
            return Ok(Argument::Null);
        }
        if let Some(color) = self.color(expr)? {
            return Ok(Argument::Color(color));
        }
        let checked = self.check(expr)?;
        Ok(if checked.column_type == ColumnType::String {
            Argument::Name(checked)
        } else if Numeric::of(&checked.column_type).is_some() {
            Argument::Number(checked)
        } else {
            Argument::Other
        })
    }

    /// A call of a function of `ColorUtilImpl`, or `None` when `function` is not one that
    /// is evaluated here.
    ///
    /// Behavior: the public static methods of `io.deephaven.engine.util.ColorUtilImpl`,
    /// with the overload `QueryLanguageParser#getMethod` chooses: one that takes `Color`
    /// for colours, `String` for names, and `long` for numbers that widen to `long`,
    /// never a mix. The overloads of `Color` and of `String` convert with `toLong` and
    /// call the one of `long`; `heatmapForeground` and `heatmapFg` then take the
    /// foreground of each. `background`, `foreground`, `backgroundForeground`,
    /// `backgroundForegroundAuto` and `heatmapForeground` have no public overload of
    /// `long`. An argument of a narrower type is converted as a cast converts it, so a
    /// null `int` is the null `long`, as seen on Deephaven 42.9 (`fg` of a null `int` is
    /// `0x84000000`).
    pub(super) fn style_call(
        &mut self,
        function: &str,
        arguments: &[Expr],
    ) -> Result<Option<Typed>, FormulaError> {
        if makes_color(function) {
            // The call is well formed or not; its value is a `Color` either way.
            self.make_color(function, arguments)?;
            return Err(refused_value());
        }
        if function != "toLong" && !STYLE_FUNCTIONS.contains(&function) {
            return Ok(None);
        }
        let no_overload = || type_error(format!("cannot find {function}() of these arguments"));
        let mut checked = Vec::with_capacity(arguments.len());
        for argument in arguments {
            checked.push(self.style_argument(argument)?);
        }
        // The first three arguments of a heat map are `double`s.
        let heat = function.starts_with("heatmap");
        let mut values = Vec::new();
        if heat {
            if checked.len() != 5 {
                return Err(no_overload());
            }
            for argument in checked.drain(..3) {
                match argument {
                    Argument::Number(value) => values.push(convert(value, &ColumnType::Double)),
                    _ => return Err(no_overload()),
                }
            }
        }
        // What remains are styles, all given in one way.
        let colors = checked
            .iter()
            .all(|argument| matches!(argument, Argument::Color(_)));
        let names = checked
            .iter()
            .all(|argument| matches!(argument, Argument::Name(_)));
        let longs = checked
            .iter()
            .all(|argument| matches!(argument, Argument::Number(value) if is_integral(value)));
        if checked.is_empty() || !(colors || names || longs) {
            return Err(
                if checked
                    .iter()
                    .any(|argument| matches!(argument, Argument::Null))
                {
                    unsupported(format!("null literal as an argument of {function}()"))
                } else {
                    no_overload()
                },
            );
        }
        let count = checked.len();
        let mut styles: Vec<Typed> = checked
            .into_iter()
            .map(|argument| match argument {
                Argument::Color(value) => value,
                Argument::Name(value) => style(ColorFn::OfName, vec![value]),
                Argument::Number(value) => convert(value, &ColumnType::Long),
                Argument::Other | Argument::Null => unreachable!("refused above"),
            })
            .collect();
        let result = |function: ColorFn, mut values: Vec<Typed>, styles: Vec<Typed>| {
            values.extend(styles);
            if matches!(
                function,
                ColorFn::IsBackgroundOverride | ColorFn::IsForegroundOverride
            ) {
                typed(
                    ColumnType::Boolean,
                    Node::Call {
                        kernel: Kernel::Color(function),
                        arguments: values,
                    },
                )
            } else {
                style(function, values)
            }
        };
        let kernel = match (function, count) {
            ("toLong", 1) if longs => ColorFn::ToLong,
            // `toLong(Color)` and `toLong(String)` are the styles the arguments are.
            ("toLong", 1) => return Ok(styles.pop()),
            ("bg", 1) => ColorFn::Background,
            ("background", 1) if !longs => ColorFn::Background,
            ("bg", 3) if longs => ColorFn::BackgroundRgb,
            ("bgo" | "backgroundOverride", 1) => ColorFn::BackgroundOverride,
            ("fg", 1) => ColorFn::Foreground,
            ("foreground", 1) if !longs => ColorFn::Foreground,
            ("fg", 3) if longs => ColorFn::ForegroundRgb,
            ("fgo" | "foregroundOverride", 1) => ColorFn::ForegroundOverride,
            ("bgfg", 2) => ColorFn::BackgroundForeground,
            ("backgroundForeground", 2) if !longs => ColorFn::BackgroundForeground,
            ("bgfg", 6) if longs => ColorFn::BackgroundForegroundRgb,
            ("bgfga", 1) => ColorFn::Auto,
            ("backgroundForegroundAuto", 1) if !longs => ColorFn::Auto,
            ("bgfga", 3) if longs => ColorFn::AutoRgb,
            ("heatmap", 2) => ColorFn::Heatmap,
            ("heatmapFg", 2) if longs => ColorFn::HeatmapForeground,
            ("heatmapFg" | "heatmapForeground", 2) if !longs => {
                styles = styles
                    .into_iter()
                    .map(|value| style(ColorFn::Foreground, vec![value]))
                    .collect();
                ColorFn::HeatmapForeground
            }
            ("isBackgroundSelectionOverride", 1) if longs => ColorFn::IsBackgroundOverride,
            ("isForegroundSelectionOverride", 1) if longs => ColorFn::IsForegroundOverride,
            _ => return Err(no_overload()),
        };
        Ok(Some(result(kernel, values, styles)))
    }
}
