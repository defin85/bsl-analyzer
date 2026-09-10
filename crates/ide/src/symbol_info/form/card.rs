use hir::{form_element_type, Builders, DefDatabase, ModuleId, Name};
use ide_db::base_db::Locale;
use ide_db::EffectiveModuleRole;
use ide_db::RootDatabaseImpl;
use symbol_info::{build_signature, CalleeKind};
use vfs::FileId;

use super::ResolvedForm;
use crate::symbol_info::{
    card_from_method_sig, collect_applied_members, file_path, module_export_members, name_eq,
    platform_receiver_members, type_label, SymbolContainer, SymbolInfoCard, SymbolInfoRequest,
    SymbolMember, SymbolMemberOrigin,
};

pub(super) fn form_card(
    db: &RootDatabaseImpl,
    symbol: &str,
    resolved: &ResolvedForm,
    req: &SymbolInfoRequest,
) -> SymbolInfoCard {
    let mut card = SymbolInfoCard::empty(symbol.to_string(), "form");
    card.container = Some(form_container(resolved));
    card.signature = Some(format!(
        "{} — {} реквизит(ов), {} элемент(ов), {} обработчик(ов)",
        resolved.qualified_form_name(),
        resolved.form.attributes().len(),
        resolved.form.elements().len(),
        resolved.form.event_handlers().len() + resolved.form.command_handlers().len()
    ));
    let mut members = if resolved.form.is_managed() {
        managed_form_members(db, resolved, req, None)
    } else {
        form_metadata_members(db, resolved, req)
    };
    members.extend(form_handler_members(resolved));
    if resolved.form.is_managed() {
        members = collect_applied_members(members, None);
    }
    card.members = members;
    card
}

fn form_metadata_members(
    db: &RootDatabaseImpl,
    resolved: &ResolvedForm,
    req: &SymbolInfoRequest,
) -> Vec<SymbolMember> {
    let fields = hir::module_implicit_fields(db, resolved.file_id);
    let mut members = Vec::new();
    for attr in resolved.form.attributes() {
        members.push(SymbolMember::metadata(
            attr.name.clone(),
            form_attribute_kind(attr),
            "form_attribute",
            form_attribute_type_from_fields(db, &fields, &attr.name, req.locale),
        ));
    }
    for element in resolved.form.elements() {
        members.push(SymbolMember::metadata(
            element.name.clone(),
            form_item_kind(element, req.locale),
            "form_element",
            form_item_member_type(db, resolved, element, req.locale),
        ));
    }
    members
}

fn form_handler_members(resolved: &ResolvedForm) -> Vec<SymbolMember> {
    let mut members = Vec::new();
    for handler in resolved.form.event_handlers() {
        members.push(SymbolMember::callable(
            handler.handler_name.clone(),
            "Обработчик",
            "handler",
            format!("{}()", handler.handler_name),
            SymbolMemberOrigin::Metadata,
        ));
    }
    for handler in resolved.form.command_handlers() {
        members.push(SymbolMember::callable(
            handler.clone(),
            "Обработчик",
            "handler",
            format!("{handler}()"),
            SymbolMemberOrigin::Metadata,
        ));
    }
    members
}

fn managed_form_members(
    db: &RootDatabaseImpl,
    resolved: &ResolvedForm,
    req: &SymbolInfoRequest,
    exact_name: Option<&str>,
) -> Vec<SymbolMember> {
    let mut members = form_metadata_members(db, resolved, req);
    match &resolved.owner {
        Some((mdo_type, object_name)) => members.extend(module_export_members(
            db,
            EffectiveModuleRole::ManagedForm,
            *mdo_type,
            object_name,
            Some(&resolved.form_name),
            req.position.map(|position| position.file_id),
        )),
        None => members.extend(base_form_export_members(db, resolved.file_id)),
    }
    for type_name in hir::managed_form_platform_type_names(&resolved.form) {
        members.extend(platform_receiver_members(
            db,
            resolved.file_id,
            db.platform_object(type_name.to_string()),
            req,
        ));
    }
    collect_applied_members(members, exact_name)
}

fn base_form_export_members(db: &RootDatabaseImpl, file_id: FileId) -> Vec<SymbolMember> {
    db.symbol_tree(ModuleId::new(file_id))
        .exported_methods()
        .map(|method| {
            let params = method
                .params
                .iter()
                .map(|param| param.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let keyword = if method.is_function { "Функция" } else { "Процедура" };
            SymbolMember::callable(
                method.name.as_str(),
                "Метод",
                "method",
                format!("{keyword} {}({params}) Экспорт", method.name.as_str()),
                SymbolMemberOrigin::Module,
            )
        })
        .collect()
}

pub(super) fn form_member_card(
    db: &RootDatabaseImpl,
    symbol: &str,
    resolved: &ResolvedForm,
    member: &str,
    req: &SymbolInfoRequest,
) -> Option<SymbolInfoCard> {
    if resolved.form.is_managed() {
        let members = managed_form_members(db, resolved, req, Some(member));
        if members.len() == 1 {
            if let Some(attr) = resolved.form.find_attribute(member) {
                return Some(form_attribute_card(db, symbol, resolved, attr, req));
            }
            if let Some(element) = resolved.form.find_element(member) {
                return Some(form_item_card(db, symbol, resolved, element, req));
            }
            if members[0].origin == SymbolMemberOrigin::Module
                && members[0].source_extension.is_none()
            {
                return form_handler_card(db, symbol, resolved, member, req);
            }
        }
        if members.is_empty() {
            return form_handler_card(db, symbol, resolved, member, req);
        }
        // A name the module declares itself stays answerable even when the managed surface
        // carries the same one: the platform form type alone brings dozens of names a handler
        // routinely reuses (`Закрыть`, `Открыть`, `ПроверитьЗаполнение`), and answering with
        // candidates alone drops the method's signature and its usages id. Module-origin
        // candidates are left to the branches above — those are the effective exports whose
        // composition the extension answers for.
        if resolved.form.find_attribute(member).is_none()
            && resolved.form.find_element(member).is_none()
            && members.iter().all(|candidate| candidate.origin != SymbolMemberOrigin::Module)
        {
            if let Some(card) = form_handler_card(db, symbol, resolved, member, req) {
                return Some(card);
            }
        }
        let mut card = SymbolInfoCard::empty(symbol.to_string(), "member candidates");
        card.container = Some(form_container(resolved));
        card.signature = Some(format!("{} candidate(s)", members.len()));
        card.members = members;
        return Some(card);
    }
    if let Some(attr) = resolved.form.find_attribute(member) {
        return Some(form_attribute_card(db, symbol, resolved, attr, req));
    }
    if let Some(element) = resolved.form.find_element(member) {
        return Some(form_item_card(db, symbol, resolved, element, req));
    }
    form_handler_card(db, symbol, resolved, member, req)
}

fn form_attribute_card(
    db: &RootDatabaseImpl,
    symbol: &str,
    resolved: &ResolvedForm,
    attr: &bsl_metadata::FormAttribute,
    req: &SymbolInfoRequest,
) -> SymbolInfoCard {
    let mut card = SymbolInfoCard::empty(symbol.to_string(), "form attribute");
    card.container = Some(form_container(resolved));
    if req.sections.type_ {
        let fields = hir::module_implicit_fields(db, resolved.file_id);
        card.return_type = form_attribute_type_from_fields(db, &fields, &attr.name, req.locale);
    }
    if attr.is_main {
        card.signature = Some("Объект — основной реквизит формы".to_string());
    }
    card
}

fn form_item_card(
    db: &RootDatabaseImpl,
    symbol: &str,
    resolved: &ResolvedForm,
    element: &bsl_metadata::FormElement,
    req: &SymbolInfoRequest,
) -> SymbolInfoCard {
    let mut card = SymbolInfoCard::empty(symbol.to_string(), "form item");
    card.container = Some(form_container(resolved));
    let kind = form_item_kind(element, req.locale);
    card.signature = Some(match element.data_path.as_deref() {
        Some(data_path) => format!("{kind}: {data_path}"),
        None => kind.clone(),
    });
    if req.sections.type_ {
        card.return_type =
            form_element_type_label(db, resolved, element, req.locale).or(Some(kind));
    }
    card
}

fn form_handler_card(
    db: &RootDatabaseImpl,
    symbol: &str,
    resolved: &ResolvedForm,
    member: &str,
    req: &SymbolInfoRequest,
) -> Option<SymbolInfoCard> {
    let module_id = ModuleId::new(resolved.file_id);
    let method = Name::new(member);
    let callee = CalleeKind::LocalMethod { module_id, method };
    let sigs = build_signature(db, resolved.file_id, &callee)?;
    let sig = sigs.first()?;
    let mut card = card_from_method_sig(db, symbol, sig, Some(form_container(resolved)), req);
    card.graph_id =
        form_handler_graph_id(db, resolved.file_id, &sig.name_russian, req.workspace_root.as_ref());
    Some(card)
}

fn form_container(resolved: &ResolvedForm) -> SymbolContainer {
    SymbolContainer {
        kind: "Форма".to_string(), name: resolved.container_name(), context: None
    }
}

fn form_attribute_kind(attr: &bsl_metadata::FormAttribute) -> String {
    if attr.is_main {
        "Реквизит формы (основной)"
    } else {
        "Реквизит формы"
    }
    .to_string()
}

fn form_attribute_type_from_fields(
    db: &RootDatabaseImpl,
    fields: &[hir::Field],
    attr_name: &str,
    locale: Locale,
) -> Option<String> {
    fields
        .iter()
        .find(|field| name_eq(field.name.as_str(), attr_name))
        .and_then(|field| type_label(db, field.ty, locale))
}

fn form_item_kind(element: &bsl_metadata::FormElement, locale: Locale) -> String {
    element
        .kind
        .base_platform_type_name()
        .unwrap_or_else(|| hir::form_element_kind_label(element.kind, locale))
        .to_string()
}

fn form_item_member_type(
    db: &RootDatabaseImpl,
    resolved: &ResolvedForm,
    element: &bsl_metadata::FormElement,
    locale: Locale,
) -> Option<String> {
    let label = form_element_type_label(db, resolved, element, locale)?;
    Some(match element.data_path.as_deref() {
        Some(data_path) => format!("{label}: {data_path}"),
        None => label,
    })
}

fn form_element_type_label(
    db: &RootDatabaseImpl,
    resolved: &ResolvedForm,
    element: &bsl_metadata::FormElement,
    locale: Locale,
) -> Option<String> {
    let ty = form_element_type(db, resolved.file_id, &resolved.form, element);
    type_label(db, ty, locale)
}

fn form_handler_graph_id(
    db: &RootDatabaseImpl,
    form_file: FileId,
    method_name: &str,
    workspace_root: Option<&crate::graph::StripRoot>,
) -> Option<String> {
    let path = file_path(db, form_file)?;
    // The graph builder encodes a form handler's fallback id relative to the WORKSPACE root, not
    // the config root — so strip exactly that root here. Without a known root an absolute path has
    // no resolvable rel, so `method_graph_id` returns `None` (no usages) rather than a wrong id.
    crate::graph::method_graph_id(&path, method_name, workspace_root)
}
