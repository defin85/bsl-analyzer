use crate::define_metadata;
use crate::metadata::*;
use crate::BodyContext;
use crate::{Diagnostic, DiagnosticCode, Fix, TextEdit};
use hir::LocalRange;
use syntax::{preproc_symbols, SyntaxKind, SyntaxToken};

pub const METADATA: DiagnosticMetadata = define_metadata! {
    diagnostic_type: DiagnosticType::CodeSmell,
    severity: DiagnosticSeverityLevel::Info,
    scope: DiagnosticScope::All,
    modules: &[],
    minutes_to_fix: 1,
    activated_by_default: true,
    compatibility_mode: DiagnosticCompatibilityMode::Undefined,
    tags: &[MetadataTag::Standard],
    can_locate_on_project: false,
    extra_min_for_complexity: 0.0,
    lsp_severity_override: "",
};

fn is_cyrillic(text: &str) -> bool {
    text.chars().any(|c| matches!(c, '\u{0400}'..='\u{04FF}'))
}

fn is_in_preprocessor(token: &SyntaxToken) -> bool {
    let mut current = token.parent();
    while let Some(node) = current {
        if matches!(
            node.kind(),
            SyntaxKind::PRE_IF_DIR
                | SyntaxKind::PRE_ELSIF_CLAUSE
                | SyntaxKind::PRE_ELSE_CLAUSE
                | SyntaxKind::PRE_SYMBOL
        ) {
            return true;
        }
        current = node.parent();
    }
    false
}

/// Каноническое написание символа препроцессора.
///
/// Список написаний и правило сравнения регистра принадлежат реестру
/// `syntax::preproc_symbols`, поэтому новый символ получает быструю правку
/// без правки этого места.
fn check_preproc_symbol(actual: &str) -> Option<String> {
    let id = preproc_symbols::lookup(actual)?;
    check_keyword(actual, &id.spellings())
}

fn check_keyword(actual: &str, canonical_forms: &[&str]) -> Option<String> {
    if canonical_forms.contains(&actual) {
        return None;
    }

    let is_cyrillic_input = is_cyrillic(actual);
    canonical_forms
        .iter()
        .find(|&&form| is_cyrillic(form) == is_cyrillic_input)
        .or_else(|| canonical_forms.first())
        .map(|&s| s.to_string())
}

fn check_token_canonical(token: &SyntaxToken) -> Option<String> {
    let actual = token.text();

    match token.kind() {
        SyntaxKind::KW_PROCEDURE => check_keyword(actual, &["Процедура", "Procedure"]),
        SyntaxKind::KW_END_PROCEDURE => check_keyword(actual, &["КонецПроцедуры", "EndProcedure"]),
        SyntaxKind::KW_FUNCTION => check_keyword(actual, &["Функция", "Function"]),
        SyntaxKind::KW_END_FUNCTION => check_keyword(actual, &["КонецФункции", "EndFunction"]),
        SyntaxKind::KW_EXPORT => check_keyword(actual, &["Экспорт", "Export"]),
        SyntaxKind::KW_VAL => check_keyword(actual, &["Знач", "Val"]),

        SyntaxKind::KW_IF => check_keyword(actual, &["Если", "If"]),
        SyntaxKind::KW_THEN => check_keyword(actual, &["Тогда", "Then"]),
        SyntaxKind::KW_ELSIF => check_keyword(actual, &["ИначеЕсли", "ElsIf"]),
        SyntaxKind::KW_ELSE => check_keyword(actual, &["Иначе", "Else"]),
        SyntaxKind::KW_END_IF => check_keyword(actual, &["КонецЕсли", "EndIf"]),

        SyntaxKind::KW_FOR => check_keyword(actual, &["Для", "For"]),
        SyntaxKind::KW_EACH => check_keyword(actual, &["Каждого", "каждого", "Each", "each"]),
        SyntaxKind::KW_IN => check_keyword(actual, &["Из", "In"]),
        SyntaxKind::KW_TO => check_keyword(actual, &["По", "To"]),
        SyntaxKind::KW_WHILE => check_keyword(actual, &["Пока", "While"]),
        SyntaxKind::KW_DO => check_keyword(actual, &["Цикл", "Do"]),
        SyntaxKind::KW_END_DO => check_keyword(actual, &["КонецЦикла", "EndDo"]),
        SyntaxKind::KW_RETURN => check_keyword(actual, &["Возврат", "Return"]),
        SyntaxKind::KW_CONTINUE => check_keyword(actual, &["Продолжить", "Continue"]),
        SyntaxKind::KW_BREAK => check_keyword(actual, &["Прервать", "Break"]),
        SyntaxKind::KW_GOTO => check_keyword(actual, &["Перейти", "Goto"]),

        SyntaxKind::KW_TRY => check_keyword(actual, &["Попытка", "Try"]),
        SyntaxKind::KW_EXCEPT => check_keyword(actual, &["Исключение", "Except"]),
        SyntaxKind::KW_END_TRY => check_keyword(actual, &["КонецПопытки", "EndTry"]),
        SyntaxKind::KW_RAISE => check_keyword(actual, &["ВызватьИсключение", "Raise"]),

        SyntaxKind::KW_VAR => check_keyword(actual, &["Перем", "Var"]),
        SyntaxKind::KW_NEW => check_keyword(actual, &["Новый", "New"]),
        SyntaxKind::KW_EXECUTE => check_keyword(actual, &["Выполнить", "Execute"]),

        SyntaxKind::KW_ADD_HANDLER => check_keyword(actual, &["ДобавитьОбработчик", "AddHandler"]),
        SyntaxKind::KW_REMOVE_HANDLER => {
            check_keyword(actual, &["УдалитьОбработчик", "RemoveHandler"])
        }

        SyntaxKind::KW_ASYNC => check_keyword(actual, &["Асинх", "Async"]),
        SyntaxKind::KW_AWAIT => check_keyword(actual, &["Ждать", "Await"]),

        SyntaxKind::KW_AND => check_keyword(actual, &["И", "And", "AND"]),
        SyntaxKind::KW_OR => check_keyword(actual, &["Или", "ИЛИ", "Or", "OR"]),
        SyntaxKind::KW_NOT => check_keyword(actual, &["Не", "НЕ", "Not", "NOT"]),

        SyntaxKind::KW_TRUE => check_keyword(actual, &["Истина", "True"]),
        SyntaxKind::KW_FALSE => check_keyword(actual, &["Ложь", "False"]),

        SyntaxKind::KW_UNDEFINED => check_keyword(actual, &["Неопределено", "Undefined"]),
        SyntaxKind::KW_NULL => check_keyword(actual, &["NULL", "Null"]),

        SyntaxKind::PRE_IF => check_keyword(actual, &["#Если", "#If"]),
        SyntaxKind::PRE_ELSIF => check_keyword(actual, &["#ИначеЕсли", "#ElsIf"]),
        SyntaxKind::PRE_ELSE => check_keyword(actual, &["#Иначе", "#Else"]),
        SyntaxKind::PRE_END_IF => check_keyword(actual, &["#КонецЕсли", "#EndIf"]),
        SyntaxKind::PRE_REGION => check_keyword(actual, &["#Область", "#Region"]),
        SyntaxKind::PRE_END_REGION => check_keyword(actual, &["#КонецОбласти", "#EndRegion"]),
        SyntaxKind::PRE_INSERT => check_keyword(actual, &["#Вставка", "#Insert"]),

        SyntaxKind::ANN_AT_CLIENT => check_keyword(actual, &["&НаКлиенте", "&AtClient"]),
        SyntaxKind::ANN_AT_SERVER => check_keyword(actual, &["&НаСервере", "&AtServer"]),
        SyntaxKind::ANN_AT_SERVER_NO_CONTEXT => {
            check_keyword(actual, &["&НаСервереБезКонтекста", "&AtServerNoContext"])
        }
        SyntaxKind::ANN_AT_CLIENT_AT_SERVER => {
            check_keyword(actual, &["&НаКлиентеНаСервере", "&AtClientAtServer"])
        }
        SyntaxKind::ANN_AT_CLIENT_AT_SERVER_NO_CONTEXT => check_keyword(
            actual,
            &["&НаКлиентеНаСервереБезКонтекста", "&AtClientAtServerNoContext"],
        ),

        SyntaxKind::IDENT if is_in_preprocessor(token) => check_preproc_symbol(actual),

        _ => None,
    }
}

pub fn check_body(ctx: &BodyContext, acc: &mut Vec<Diagnostic<LocalRange>>) {
    let code = DiagnosticCode::CanonicalSpellingKeywords;
    if ctx.is_disabled_with_metadata(code) {
        return;
    }

    let mut diagnostics = Vec::new();

    for token in ctx.tokens() {
        {
            if matches!(
                token.kind(),
                kind if kind.is_trivia()
            ) {
                continue;
            }

            if let Some(canonical) = check_token_canonical(&token) {
                let range = LocalRange::of_detached_node(token.text_range());
                diagnostics.push(Diagnostic {
                    code: DiagnosticCode::CanonicalSpellingKeywords,
                    message: format!(
                        "Ключевое слово '{}' написано не канонически, используйте '{}'",
                        token.text(),
                        canonical
                    ),
                    severity: ctx.severity(code),
                    range,
                    tags: ctx.tags(code),
                    fixes: vec![Fix::safe(
                        format!("Заменить на '{}'", canonical),
                        vec![TextEdit { range, new_text: canonical }],
                    )],
                });
            }
        }
    }

    acc.extend(diagnostics);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::check_body_diagnostic_with_config;
    use crate::DiagnosticsConfig;
    /// Каждый символ реестра получает быструю правку канонического
    /// написания — и русскую, и английскую.
    ///
    /// Обход реестра, а не список руками: ровно из-за списка руками
    /// `МобильныйАвтономныйСервер` правки не получал, хотя известным
    /// символом считался.
    #[test]
    fn every_registry_symbol_gets_a_canonical_spelling_fix() {
        use syntax::preproc_symbols::PreprocSymbolId;

        for &id in PreprocSymbolId::ALL {
            for canonical in id.spellings() {
                let written = canonical.to_lowercase();
                assert_ne!(written, canonical, "{canonical:?}: вход совпал с каноном");
                let code =
                    format!("Процедура Тест()\n#Если {written} Тогда\n#КонецЕсли\nКонецПроцедуры");
                let diagnostics = check_body_diagnostic_with_config(
                    &code,
                    DiagnosticsConfig::default(),
                    check_body,
                );
                let offered: Vec<&str> = diagnostics
                    .iter()
                    .filter(|d| d.code == DiagnosticCode::CanonicalSpellingKeywords)
                    .flat_map(|d| &d.fixes)
                    .flat_map(|fix| &fix.edits)
                    .map(|edit| edit.new_text.as_str())
                    .collect();
                assert!(
                    offered.contains(&canonical),
                    "{written:?}: правка на {canonical:?} не предложена, предложено {offered:?}"
                );
            }
        }
    }

    /// Символ, которого реестр не знает, правки не получает: иначе
    /// диагностика предлагала бы канон для написания без источника.
    #[test]
    fn symbols_outside_the_registry_get_no_fix() {
        for written in ["linux", "windows", "macos", "нечто"] {
            let code =
                format!("Процедура Тест()\n#Если {written} Тогда\n#КонецЕсли\nКонецПроцедуры");
            let diagnostics =
                check_body_diagnostic_with_config(&code, DiagnosticsConfig::default(), check_body);
            assert!(
                !diagnostics.iter().any(|d| d.code == DiagnosticCode::CanonicalSpellingKeywords),
                "{written:?}: предложена правка написанию вне реестра: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn test_canonical_keywords() {
        let code = r#"Процедура Тест()
    Если Истина Тогда
        Возврат 1;
    КонецЕсли;
    Возврат 0;
КонецПроцедуры"#;

        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);

        assert_eq!(diagnostics.len(), 0);
    }

    #[test]
    fn test_canonical_insert_directive_not_flagged() {
        // `#Вставка` / `#КонецВставки` are the canonical configuration-extension directives;
        // they must NOT be flagged as a misspelling of a non-existent `#Вставить`.
        let code = "Процедура Тест()\n#Вставка\n\tА = 1;\n#КонецВставки\nКонецПроцедуры";

        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);

        assert!(
            !diagnostics.iter().any(|d| d.code == DiagnosticCode::CanonicalSpellingKeywords),
            "canonical `#Вставка` must not be flagged; got {diagnostics:?}"
        );
    }

    #[test]
    fn test_lowercase_insert_directive_flagged() {
        // Lowercase `#вставка` is non-canonical and is still flagged.
        let code = "Процедура Тест()\n#вставка\n\tА = 1;\n#КонецВставки\nКонецПроцедуры";

        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);

        assert!(
            diagnostics.iter().any(|d| d.code == DiagnosticCode::CanonicalSpellingKeywords),
            "lowercase `#вставка` should be flagged; got {diagnostics:?}"
        );
    }

    #[test]
    fn test_lowercase_keywords() {
        let code = r#"функция Тест()
    возврат 0;
конецфункции"#;

        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);

        assert!(diagnostics.len() >= 3);
        assert_eq!(diagnostics[0].code, DiagnosticCode::CanonicalSpellingKeywords);
    }

    #[test]
    fn test_uppercase_keywords() {
        let code = r#"ФУНКЦИЯ Тест()
    ВОЗВРАТ 0;
КОНЕЦФУНКЦИИ"#;

        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);

        assert!(diagnostics.len() >= 3);
    }

    #[test]
    fn test_mixed_case_keywords() {
        let code = r#"ЕслИ Истина ТогдА
    Результат = 1;
ИнаЧе
    Результат = 0;
КонецЕсЛи;"#;

        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);

        assert!(diagnostics.len() >= 4);
    }

    #[test]
    fn test_logical_operators() {
        let code = r#"Процедура Тест()
    // Canonical forms
    А = Истина И Ложь;
    Б = Истина And Ложь;
    В = Истина AND Ложь;
    Г = Истина Or Ложь;
    Д = Истина OR Ложь;
    Е = Истина Not Ложь;
    Ж = Истина NOT Ложь;

    // Non-canonical forms
    З = Истина и Ложь;
    И = Истина and Ложь;
    К = Истина or Ложь;
    Л = Истина not Ложь;
КонецПроцедуры"#;

        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);

        assert!(!diagnostics.is_empty(), "Should detect lowercase logical operators");
    }

    #[test]
    fn test_each_keyword_lowercase() {
        let code = r#"Процедура Тест()
    // Lowercase "каждого" is canonical after "Для"
    Для Каждого Элемент Из Массив Цикл
        Сообщить(Элемент);
    КонецЦикла;

    Для каждого Элемент Из Массив Цикл
        Сообщить(Элемент);
    КонецЦикла;
КонецПроцедуры"#;

        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);

        for diag in &diagnostics {
            assert!(!diag.message.contains("Каждого"));
            assert!(!diag.message.contains("каждого"));
        }
    }

    #[test]
    fn test_non_canonical_var_keyword() {
        let code = r#"Процедура Тест()
    ПерЕМ Б;
КонецПроцедуры"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics.iter().any(|d| d.message.contains("ПерЕМ")));
    }

    #[test]
    fn test_non_canonical_undefined_and_new() {
        let code = r#"Процедура Тест()
    А = НЕОПРЕделено;
    Б = НоВый Массив();
КонецПроцедуры"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics.iter().any(|d| d.message.contains("НЕОПРЕделено")));
        assert!(diagnostics.iter().any(|d| d.message.contains("НоВый")));
    }

    #[test]
    fn test_non_canonical_if_keywords() {
        let code = r#"Процедура Тест()
    ЕслИ x % 15 = 0 ТогдА
        Результат = "FizzBuzz";
    ИначеЕСли x % 3 = 0 Тогда
        Результат = "Fizz";
    ИнаЧе
        Результат = x;
    КонецЕсЛи;
КонецПроцедуры"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics.iter().any(|d| d.message.contains("ЕслИ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ТогдА")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ИначеЕСли")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ИнаЧе")));
        assert!(diagnostics.iter().any(|d| d.message.contains("КонецЕсЛи")));
    }

    #[test]
    fn test_canonical_if_keywords_no_diagnostic() {
        let code = r#"Процедура Тест()
    Если x % 15 = 0 Тогда
        Результат = "FizzBuzz";
    ИначеЕсли x % 3 = 0 Тогда
        Результат = "Fizz";
    Иначе
        Результат = x;
    КонецЕсли;
КонецПроцедуры"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert_eq!(diagnostics.len(), 0);
    }

    #[test]
    fn test_non_canonical_for_each_loop() {
        let code = r#"Процедура Тест()
    ДЛЯ КАЖДОГО СтрокаДанных ИЗ x ЦикЛ
       ПРервать;
       ПРодолжить;
    КонецЦиклА;
КонецПроцедуры"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics.iter().any(|d| d.message.contains("ДЛЯ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("КАЖДОГО")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ИЗ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ЦикЛ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ПРервать")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ПРодолжить")));
        assert!(diagnostics.iter().any(|d| d.message.contains("КонецЦиклА")));
    }

    #[test]
    fn test_canonical_for_each_loop_no_diagnostic() {
        let code = r#"Процедура Тест()
    Для Каждого СтрокаДанных Из x Цикл
        Прервать;
        Продолжить;
    КонецЦикла;
КонецПроцедуры"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert_eq!(diagnostics.len(), 0);
    }

    #[test]
    fn test_non_canonical_exception_handling() {
        let code = r#"Процедура Тест()
    ПопЫтка
        А = Б;
    ИсключенИЕ
        ВызваТЬИсключение "Исключение";
    КОНЕЦПопытки;
КонецПроцедуры"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics.iter().any(|d| d.message.contains("ПопЫтка")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ИсключенИЕ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ВызваТЬИсключение")));
        assert!(diagnostics.iter().any(|d| d.message.contains("КОНЕЦПопытки")));
    }

    #[test]
    fn test_non_canonical_procedure_and_function() {
        let code = r#"ПРОЦЕДУРА Тест3(ЗнаЧ Параметр) ЭКспорт
КонецПРоцедуры

ФункцИЯ Тест4()
    ВозВРат Истина;
КонецФункцИИ"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics.iter().any(|d| d.message.contains("ПРОЦЕДУРА")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ЗнаЧ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ЭКспорт")));
        assert!(diagnostics.iter().any(|d| d.message.contains("КонецПРоцедуры")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ФункцИЯ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ВозВРат")));
        assert!(diagnostics.iter().any(|d| d.message.contains("КонецФункцИИ")));
    }

    #[test]
    fn test_non_canonical_preprocessor_directives() {
        let code = "#ЕСЛИ СеРвер ТОГДА\n#ИнАЧе\n#КонецЕСЛИ";
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics
            .iter()
            .any(|d| d.message.contains("ЕСЛИ") || d.message.contains("#ЕСЛИ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ТОГДА")));
    }

    #[test]
    fn test_canonical_preprocessor_directives_no_diagnostic() {
        let code = "#Если Сервер Или Клиент Тогда\n#Иначе\n#КонецЕсли";
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert_eq!(diagnostics.len(), 0);
    }

    #[test]
    fn test_non_canonical_annotations() {
        let code = "&НАСервере\nПроцедура Тест()\nКонецПроцедуры";
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert_eq!(diagnostics.len(), 1);
        assert!(diagnostics[0].message.contains("НАСервере"));
    }

    #[test]
    fn test_canonical_annotations_no_diagnostic() {
        let code = "&НаСервере\nПроцедура Тест()\nКонецПроцедуры";
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert_eq!(diagnostics.len(), 0);
    }

    #[test]
    fn test_non_canonical_region_directives() {
        let code = "#ОБЛАСТЬ НоваяОбласть\n#КонецОбластИ";
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics.iter().any(|d| d.message.contains("ОБЛАСТЬ")));
        assert!(diagnostics.iter().any(|d| d.message.contains("КонецОбластИ")));
    }

    #[test]
    fn test_non_canonical_logical_operators() {
        let code = r#"Процедура Тест()
    А = А и А ИлИ А И нЕ А;
    А = ЛОЖЬ;
    А = ИсТИна;
КонецПроцедуры"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(!diagnostics.is_empty(), "Should detect non-canonical logical operators");
    }

    #[test]
    fn test_english_non_canonical_keywords() {
        let code = r#"PROCEDURE Test7(VaL Param) ExPort
EndPROCedure

FUNCtion Test8()
    RETUrn True;
EnDFunction"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert!(diagnostics.iter().any(|d| d.message.contains("PROCEDURE")));
        assert!(diagnostics.iter().any(|d| d.message.contains("VaL")));
        assert!(diagnostics.iter().any(|d| d.message.contains("ExPort")));
        assert!(diagnostics.iter().any(|d| d.message.contains("EndPROCedure")));
        assert!(diagnostics.iter().any(|d| d.message.contains("FUNCtion")));
        assert!(diagnostics.iter().any(|d| d.message.contains("RETUrn")));
        assert!(diagnostics.iter().any(|d| d.message.contains("EnDFunction")));
    }

    #[test]
    fn test_english_canonical_keywords_no_diagnostic() {
        let code = r#"Procedure Test5(Val Param) Export
EndProcedure

Function Test6()
    Return True;
EndFunction"#;
        let config = DiagnosticsConfig::default();
        let diagnostics = check_body_diagnostic_with_config(code, config, check_body);
        assert_eq!(diagnostics.len(), 0);
    }
}
