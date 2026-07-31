use crate::{load_config, save_config, AppConfig};
use anyhow::{Context, Result};
use crossterm::event::{DisableBracketedPaste, EnableBracketedPaste};
use crossterm::terminal::{Clear, ClearType};
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind, MouseButton, MouseEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear as TuiClear, List, ListItem, ListState, Paragraph},
    Terminal,
};
use std::io;
use util_layer::{ConfigModeConfig, Endpoint, EndpointConfig};

pub fn run(config: ConfigModeConfig) -> Result<()> {
    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, Clear(ClearType::All), EnableMouseCapture, EnableBracketedPaste)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // create app and run it
    let res = run_app(&mut terminal, config.config_path);

    terminal.clear()?;
    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableBracketedPaste,
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{:?}", err)
    }

    Ok(())
}

pub(crate) enum InputMode {
    Normal,
    Editing,
    ConfirmDelete,
    HttpWarning,
}

pub(crate) enum Focus {
    Endpoints,
    Fields,
}

#[derive(Clone, PartialEq)]
pub(crate) enum Field {
    Name,
    AccessKey,
    SecretKey,
    Url,
    Region,
}

pub(crate) struct App {
    pub(crate) config: AppConfig,
    pub(crate) config_path: Option<std::path::PathBuf>,
    pub(crate) list_state: ListState,
    pub(crate) input_mode: InputMode,
    pub(crate) focus: Focus,
    pub(crate) selected_field: Field,
    pub(crate) edit_buffer: String,
    pub(crate) show_help: bool,
    pub(crate) endpoint_areas: Vec<Rect>,
    pub(crate) field_areas: Vec<(Field, Rect)>,
    pub(crate) button_areas: std::collections::HashMap<String, Rect>,
}

impl App {
    pub(crate) fn new(config: AppConfig, config_path: Option<std::path::PathBuf>) -> App {
        let mut list_state = ListState::default();
        if !config.endpoints.is_empty() {
            list_state.select(Some(0));
        }
        App {
            config,
            config_path,
            list_state,
            input_mode: InputMode::Normal,
            focus: Focus::Endpoints,
            selected_field: Field::Name,
            edit_buffer: String::new(),
            show_help: false,
            endpoint_areas: Vec::new(),
            field_areas: Vec::new(),
            button_areas: std::collections::HashMap::new(),
        }
    }

    pub(crate) fn next_endpoint(&mut self) {
        let i = match self.list_state.selected() {
            Some(i) => {
                if i >= self.config.endpoints.len().saturating_sub(1) {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        if !self.config.endpoints.is_empty() {
            self.list_state.select(Some(i));
        }
    }

    fn previous_endpoint(&mut self) {
        let i = match self.list_state.selected() {
            Some(i) => {
                if i == 0 {
                    self.config.endpoints.len().saturating_sub(1)
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        if !self.config.endpoints.is_empty() {
            self.list_state.select(Some(i));
        }
    }

    fn next_field(&mut self) {
        self.selected_field = match self.selected_field {
            Field::Name => Field::AccessKey,
            Field::AccessKey => Field::SecretKey,
            Field::SecretKey => Field::Url,
            Field::Url => Field::Region,
            Field::Region => Field::Name,
        };
    }

    fn previous_field(&mut self) {
        self.selected_field = match self.selected_field {
            Field::Name => Field::Region,
            Field::AccessKey => Field::Name,
            Field::SecretKey => Field::AccessKey,
            Field::Url => Field::SecretKey,
            Field::Region => Field::Url,
        };
    }

    fn add_endpoint(&mut self) -> Result<()> {
        let mut name = "name".to_string();
        let mut i = 1;
        while self.config.endpoints.iter().any(|e| e.name == name) {
            name = format!("name{}", i);
            i += 1;
        }

        let new_endpoint = Endpoint {
            name,
            config: Some(EndpointConfig::default()),
            settings: None,
        };
        self.config.endpoints.push(new_endpoint);
        self.list_state.select(Some(self.config.endpoints.len() - 1));
        save_config(&self.config, self.config_path.clone())
    }

    fn delete_endpoint(&mut self) -> Result<()> {
        if let Some(_) = self.list_state.selected() {
            self.input_mode = InputMode::ConfirmDelete;
        }
        Ok(())
    }

    fn confirm_delete(&mut self) -> Result<()> {
        if let Some(i) = self.list_state.selected() {
            if i < self.config.endpoints.len() {
                self.config.endpoints.remove(i);
                if self.config.endpoints.is_empty() {
                    self.list_state.select(None);
                } else if i >= self.config.endpoints.len() {
                    self.list_state.select(Some(self.config.endpoints.len() - 1));
                }
                save_config(&self.config, self.config_path.clone())?;
            }
        }
        self.input_mode = InputMode::Normal;
        Ok(())
    }

    pub(crate) fn start_editing(&mut self) {
        if let Some(i) = self.list_state.selected() {
            let endpoint = &self.config.endpoints[i];
            self.edit_buffer = match self.selected_field {
                Field::Name => endpoint.name.clone(),
                Field::AccessKey => endpoint.config.as_ref().map(|c| c.s3_access_key.clone()).unwrap_or_default(),
                Field::SecretKey => endpoint.config.as_ref().map(|c| c.s3_secret_key.clone()).unwrap_or_default(),
                Field::Url => endpoint.config.as_ref().map(|c| c.s3_url.clone()).unwrap_or_default(),
                Field::Region => endpoint.config.as_ref().map(|c| c.s3_region.clone()).unwrap_or_default(),
            };
            self.input_mode = InputMode::Editing;
        }
    }

    pub(crate) fn save_edit(&mut self) -> Result<()> {
        if let Some(i) = self.list_state.selected() {
            match self.selected_field {
                Field::Name => {
                    let new_name = self.edit_buffer.trim().to_string();
                    if new_name.is_empty() || !new_name.chars().all(|c| c.is_ascii_alphanumeric()) {
                        return Ok(()); // Or handle error
                    }
                    // Check for duplicate names (excluding current endpoint)
                    let is_duplicate = self.config.endpoints.iter()
                        .enumerate()
                        .any(|(idx, e)| idx != i && e.name == new_name);

                    if is_duplicate {
                        // For now, we just don't save. Ideally we'd show an error message.
                        return Ok(());
                    }
                    self.config.endpoints[i].name = new_name;
                }
                Field::AccessKey => {
                    let endpoint = &mut self.config.endpoints[i];
                    if endpoint.config.is_none() { endpoint.config = Some(EndpointConfig::default()); }
                    endpoint.config.as_mut().unwrap().s3_access_key = self.edit_buffer.trim().to_string();
                }
                Field::SecretKey => {
                    let endpoint = &mut self.config.endpoints[i];
                    if endpoint.config.is_none() { endpoint.config = Some(EndpointConfig::default()); }
                    endpoint.config.as_mut().unwrap().s3_secret_key = self.edit_buffer.trim().to_string();
                }
                Field::Url => {
                    let new_url = self.edit_buffer.trim().to_string();
                    let is_http = matches!(util_layer::url_parser::validate_s3_endpoint(&new_url), util_layer::url_parser::S3EndpointType::HTTP);
                    
                    let endpoint = &mut self.config.endpoints[i];
                    if endpoint.config.is_none() { endpoint.config = Some(EndpointConfig::default()); }
                    endpoint.config.as_mut().unwrap().s3_url = self.edit_buffer.trim().to_string();

                    if is_http {
                        self.input_mode = InputMode::HttpWarning;
                        return Ok(());
                    }
                }
                Field::Region => {
                    let endpoint = &mut self.config.endpoints[i];
                    if endpoint.config.is_none() { endpoint.config = Some(EndpointConfig::default()); }
                    endpoint.config.as_mut().unwrap().s3_region = self.edit_buffer.trim().to_string();
                }
            }
        }
        self.input_mode = InputMode::Normal;
        save_config(&self.config, self.config_path.clone())
    }
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, config_path: Option<std::path::PathBuf>) -> Result<()> 
where
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let config = load_config(config_path.clone())
        .with_context(|| format!("Failed to load config"))?;
    let mut app = App::new(config, config_path);

    if app.config.endpoints.is_empty() {
        app.add_endpoint()?;
        app.focus = Focus::Fields;
        app.selected_field = Field::Name;
        app.edit_buffer = app.config.endpoints.last().unwrap().name.clone();
        app.input_mode = InputMode::Editing;
    }

    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        let event = event::read()?;
        if let Event::Paste(text) = event {
            if let InputMode::Editing = app.input_mode {
                if app.selected_field == Field::Name {
                    app.edit_buffer
                        .extend(text.chars().filter(|c| c.is_ascii_alphanumeric()));
                } else {
                    app.edit_buffer.push_str(&text);
                }
            }
        } else if let Event::Key(key) = event {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match app.input_mode {
                InputMode::Normal => match key.code {
                    KeyCode::Char('q') => {
                        return Ok(());
                    }
                    KeyCode::Char('a') => {
                        app.add_endpoint()?;
                        app.focus = Focus::Fields;
                        app.selected_field = Field::Name;
                        app.edit_buffer = app.config.endpoints.last().unwrap().name.clone();
                        app.input_mode = InputMode::Editing;
                    }
                    KeyCode::Char('d') => {
                        app.delete_endpoint()?;
                    }
                    KeyCode::Char('h') => app.show_help = !app.show_help,
                    KeyCode::Down | KeyCode::Tab => {
                        match app.focus {
                            Focus::Endpoints => {
                                app.next_endpoint();
                            }
                            Focus::Fields => {
                                app.next_field();
                                app.start_editing();
                            }
                        }
                    }
                    KeyCode::Up | KeyCode::BackTab => {
                        match app.focus {
                            Focus::Endpoints => {
                                app.previous_endpoint();
                            }
                            Focus::Fields => {
                                app.previous_field();
                                app.start_editing();
                            }
                        }
                    }
                    KeyCode::Right => {
                        match app.focus {
                            Focus::Endpoints => {
                                app.focus = Focus::Fields;
                                app.start_editing();
                            }
                            Focus::Fields => app.start_editing(),
                        }
                    }
                    KeyCode::Left => {
                        match app.focus {
                            Focus::Endpoints => {}
                            Focus::Fields => {
                                app.focus = Focus::Endpoints;
                            }
                        }
                    }
                    KeyCode::Enter => app.start_editing(),
                    _ => {}
                },
                InputMode::ConfirmDelete => match key.code {
                    KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => {
                        app.confirm_delete()?;
                    }
                    KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                        app.input_mode = InputMode::Normal;
                    }
                    _ => {}
                },
                InputMode::HttpWarning => match key.code {
                    KeyCode::Enter | KeyCode::Esc | KeyCode::Char(' ') => {
                        app.input_mode = InputMode::Normal;
                        save_config(&app.config, app.config_path.clone())?;
                    }
                    _ => {}
                },
                InputMode::Editing => match key.code {
                    KeyCode::Enter => {
                        app.save_edit()?;
                    }
                    KeyCode::Esc => {
                        app.input_mode = InputMode::Normal;
                        app.edit_buffer.clear();
                    }
                    KeyCode::Left => {
                        app.save_edit()?;
                        app.focus = Focus::Endpoints;
                        app.input_mode = InputMode::Normal;
                    }
                    KeyCode::Down | KeyCode::Tab => {
                        app.save_edit()?;
                        app.next_field();
                        app.start_editing();
                    }
                    KeyCode::Up | KeyCode::BackTab => {
                        app.save_edit()?;
                        app.previous_field();
                        app.start_editing();
                    }
                    KeyCode::Char('k') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                        app.edit_buffer.clear();
                    }
                    KeyCode::Char(c) => {
                        if app.selected_field == Field::Name {
                            if c.is_ascii_alphanumeric() {
                                app.edit_buffer.push(c);
                            }
                        } else {
                            app.edit_buffer.push(c);
                        }
                    }
                    KeyCode::Backspace => {
                        app.edit_buffer.pop();
                    }
                    _ => {}
                },
            }
        } else if let Event::Mouse(mouse) = event {
            if mouse.kind == MouseEventKind::Down(MouseButton::Left) {
                // Check footer buttons
                let mut button_clicked = None;
                for (id, area) in &app.button_areas {
                    if mouse.column >= area.x && mouse.column < area.x + area.width &&
                       mouse.row >= area.y && mouse.row < area.y + area.height {
                        button_clicked = Some(id.clone());
                        break;
                    }
                }

                if let Some(id) = button_clicked {
                    match id.as_str() {
                        "quit" => return Ok(()),
                        "add" => {
                            app.add_endpoint()?;
                            app.focus = Focus::Fields;
                            app.selected_field = Field::Name;
                            app.edit_buffer = app.config.endpoints.last().unwrap().name.clone();
                            app.input_mode = InputMode::Editing;
                        }
                        "delete" => {
                            app.delete_endpoint()?;
                        }
                        "enter" => {
                            if let InputMode::Editing = app.input_mode {
                                app.save_edit()?;
                            } else {
                                app.start_editing();
                            }
                        }
                        "cancel" => {
                            if let InputMode::Editing = app.input_mode {
                                app.input_mode = InputMode::Normal;
                                app.edit_buffer.clear();
                            }
                        }
                        "clear" => {
                            if let InputMode::Editing = app.input_mode {
                                app.edit_buffer.clear();
                            }
                        }
                        "confirm_yes" => {
                            if let InputMode::ConfirmDelete = app.input_mode {
                                app.confirm_delete()?;
                            }
                        }
                        "confirm_no" => {
                            if let InputMode::ConfirmDelete = app.input_mode {
                                app.input_mode = InputMode::Normal;
                            }
                        }
                        "help" | "help_toggle_inner" => {
                            app.show_help = !app.show_help;
                        }
                        _ => {}
                    }
                }

                    // Check endpoints
                if !app.show_help {
                    let mut endpoint_to_select = None;
                    let offset = app.list_state.offset();
                    for (idx, area) in app.endpoint_areas.iter().enumerate() {
                        if mouse.column >= area.x && mouse.column < area.x + area.width &&
                           mouse.row >= area.y && mouse.row < area.y + area.height {
                            endpoint_to_select = Some(idx + offset);
                            break;
                        }
                    }

                    if let Some(idx) = endpoint_to_select {
                        if idx < app.config.endpoints.len() {
                            if let InputMode::Editing = app.input_mode {
                                app.save_edit()?;
                            }
                            app.list_state.select(Some(idx));
                            app.focus = Focus::Endpoints;
                            app.input_mode = InputMode::Normal;
                        }
                    }

                    // Check fields
                    let mut field_to_edit = None;
                    for (field, area) in &app.field_areas {
                        if mouse.column >= area.x && mouse.column < area.x + area.width &&
                           mouse.row >= area.y && mouse.row < area.y + area.height {
                            field_to_edit = Some(field.clone());
                            break;
                        }
                    }

                    if let Some(field) = field_to_edit {
                        if let InputMode::Editing = app.input_mode {
                            app.save_edit()?;
                        }
                        app.selected_field = field;
                        app.focus = Focus::Fields;
                        app.start_editing();
                    }
                }
            }
        }
    }
}

fn ui(f: &mut ratatui::Frame, app: &mut App) {
    app.endpoint_areas.clear();
    app.field_areas.clear();
    app.button_areas.clear();

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Length(3),
                Constraint::Min(0),
                Constraint::Length(3),
            ]
            .as_ref(),
        )
        .split(f.area());

    let title = Paragraph::new("SnowCopy Configuration Manager")
        .style(Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(title, chunks[0]);

    if app.show_help {
        let help_button_style = Style::default().bg(Color::Blue).fg(Color::White);
        let help_text = vec![
            Line::from(vec![
                Span::raw("S3 servers, fields and buttons are clickable in most modern terminals."),
            ]),
            Line::from(vec![
                Span::raw("Like this button to toggle "),
                Span::styled(" help ", help_button_style),
            ]),
            Line::from(""),
            Line::from(vec![Span::styled("Navigation:", Style::default().add_modifier(Modifier::BOLD))]),
            Line::from("  - Up/Down, Tab/Shift+Tab: Navigate S3 servers (left) or fields (right)"),
            Line::from("  - Left/Right: Switch focus between S3 servers and fields"),
            Line::from("  - Enter: Edit selected field"),
            Line::from("  - Esc: Cancel editing"),
            Line::from("  - Ctrl-k: Clear field (in edit mode)"),
            Line::from("  - a: Add new S3 server"),
            Line::from("  - d: Delete selected S3 server"),
            Line::from("  - q: Quit"),
            Line::from("  - h: Toggle help"),
        ];

        let help = Paragraph::new(help_text)
            .block(Block::default().title("Help").borders(Borders::ALL));
        f.render_widget(help, chunks[1]);
        
        // Add the help toggle button area for the button in the help text
        // The button is " help " which is 6 chars long
        // It's in the second line of the help text.
        // help_text[1] is: "like this button to toggle " (27 chars) + " help " (6 chars)
        let help_area = chunks[1];
        app.button_areas.insert("help_toggle_inner".to_string(), Rect {
            x: help_area.x + 1 + 27,
            y: help_area.y + 1 + 1, // Second line (0-indexed)
            width: 6,
            height: 1,
        });
    } else {
        let body_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(30), Constraint::Percentage(70)].as_ref())
            .split(chunks[1]);

    // Endpoints List
    let list_area = body_chunks[0];
    let list_height = list_area.height.saturating_sub(2); // Subtract borders
    let offset = app.list_state.offset();

    let items: Vec<ListItem> = app
        .config
        .endpoints
        .iter()
        .enumerate()
        .map(|(idx, e)| {
            // Calculate if item is visible and its position
            if idx >= offset && idx < offset + list_height as usize {
                let item_rect = Rect {
                    x: list_area.x + 1,
                    y: list_area.y + 1 + (idx - offset) as u16,
                    width: list_area.width.saturating_sub(2),
                    height: 1,
                };
                app.endpoint_areas.push(item_rect);
            }

            let mut label = e.name.clone();
            let mut style = Style::default();
            
            // Check for duplicate names
            let is_duplicate = app.config.endpoints.iter()
                .enumerate()
                .any(|(other_idx, other_e)| idx != other_idx && e.name == other_e.name);

            if is_duplicate {
                label = format!("🔺 {}", label);
                style = style.fg(Color::Red);
            } else if !e.is_complete() {
                label = format!("⚠ {}", label);
                style = style.fg(Color::Yellow);
            } else if let Some(config) = &e.config {
                if is_http(config) {
                    label = format!("{}", label);
                    style = style.fg(Color::LightYellow);
                }
            }
            ListItem::new(label).style(style)
        })
        .collect();
    let endpoint_list_style = match app.focus {
        Focus::Endpoints => Style::default()
            .bg(Color::White)
            .fg(Color::Black),
        Focus::Fields => Style::default()
            .bg(Color::White)
            .fg(Color::Black),
    };

    let list = List::new(items)
        .block(Block::default().title("S3 servers").borders(Borders::ALL))
        .highlight_style(endpoint_list_style)
        .highlight_symbol(">> ");
    f.render_stateful_widget(list, body_chunks[0], &mut app.list_state);

    // Endpoint details
    if let Some(i) = app.list_state.selected() {
        if i < app.config.endpoints.len() {
            let endpoint = &app.config.endpoints[i];
            let details_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(0),
                    ]
                    .as_ref(),
                )
                .split(body_chunks[1]);

            let fields = [
                (Field::Name, "Name", endpoint.name.as_str()),
                (Field::AccessKey, "S3 Access Key", endpoint.config.as_ref().map(|c| c.s3_access_key.as_str()).unwrap_or("")),
                (Field::SecretKey, "S3 Secret Key", endpoint.config.as_ref().map(|c| c.s3_secret_key.as_str()).unwrap_or("")),
                (Field::Url, "S3 URL", endpoint.config.as_ref().map(|c| c.s3_url.as_str()).unwrap_or("")),
                (Field::Region, "S3 Region (non-aws s3 servers commonly use us-east-1)", endpoint.config.as_ref().map(|c| c.s3_region.as_str()).unwrap_or("")),
            ];

            for (idx, (field, label, value)) in fields.iter().enumerate() {
                let is_selected = std::mem::discriminant(&app.selected_field) == std::mem::discriminant(field);
                let mut style = Style::default();
                
                let is_duplicate_name = if let Field::Name = field {
                    let name_to_check = if is_selected && matches!(app.input_mode, InputMode::Editing) {
                        app.edit_buffer.trim()
                    } else {
                        endpoint.name.as_str()
                    };

                    !name_to_check.is_empty() && app.config.endpoints.iter()
                        .enumerate()
                        .any(|(other_idx, other_e)| i != other_idx && other_e.name == name_to_check)
                } else {
                    false
                };

                let display_value: &str = if is_selected {
                    match app.input_mode {
                        InputMode::Editing => app.edit_buffer.as_str(),
                        InputMode::Normal | InputMode::ConfirmDelete | InputMode::HttpWarning => *value,
                    }
                } else {
                    *value
                };

                let is_incomplete = match field {
                    Field::Name => display_value.trim().is_empty(),
                    Field::AccessKey | Field::SecretKey | Field::Url => {
                        display_value.trim().is_empty()
                    }
                    Field::Region => false,
                };

                let is_invalid_url = if let Field::Url = field {
                    matches!(util_layer::url_parser::validate_s3_endpoint(display_value), util_layer::url_parser::S3EndpointType::Invalid) && !display_value.trim().is_empty()
                } else {
                    false
                };

                let is_http_url = if let Field::Url = field {
                    matches!(util_layer::url_parser::validate_s3_endpoint(display_value), util_layer::url_parser::S3EndpointType::HTTP)
                } else {
                    false
                };

                if is_selected {
                    match app.focus {
                        Focus::Fields => style = style.fg(Color::Green).add_modifier(Modifier::BOLD),
                        _ => {
                            if let InputMode::Editing = app.input_mode {
                                style = style.fg(Color::Yellow).add_modifier(Modifier::BOLD);
                            }
                        }
                    }
                }

                if is_duplicate_name {
                    style = style.fg(Color::Red);
                } else if is_incomplete || is_http_url || is_invalid_url {
                    style = style.fg(Color::LightYellow);
                }

                let mut block = Block::default().title(*label).borders(Borders::ALL);
                if is_duplicate_name {
                    block = block.title(" Change! Names must be unique.");
                } else if is_invalid_url {
                    block = block.title("🔺 Invalid URL");
                } else if is_http_url {
                    block = block.title("⚠ Insecure HTTP Protocol");
                } else if is_incomplete {
                    block = block.title("⚠");
                }

                let p = Paragraph::new(display_value)
                    .style(style)
                    .block(block);
                let area = details_chunks[idx];
                app.field_areas.push((field.clone(), area));
                f.render_widget(p, area);

                if is_selected {
                    if let InputMode::Editing = app.input_mode {
                        f.set_cursor_position((
                            details_chunks[idx].x + display_value.len() as u16 + 1,
                            details_chunks[idx].y + 1,
                        ));
                    }
                }
            }

            //Show example for up and down
            let name = if matches!(app.selected_field, Field::Name) && matches!(app.input_mode, InputMode::Editing) {
                app.edit_buffer.as_str()
            } else {
                endpoint.name.as_str()
            };
            let example_text = format!(
                "Examples\nup:   snowcopy my-local-folder {}:my-s3-bucket\ndown: snowcopy {}:my-s3-bucket my-local-folder",
                name, name
            );
            let example = Paragraph::new(example_text)
                .style(Style::default().fg(Color::DarkGray))
                .block(Block::default().borders(Borders::NONE));
            
            let example_area = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(0), Constraint::Length(3), Constraint::Length(1)].as_ref())
                .split(details_chunks[5])[1];
            
            let example_area = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Length(1), Constraint::Min(0)].as_ref())
                .split(example_area)[1];

            f.render_widget(example, example_area);
        }
    }
}

    let footer_area = chunks[2];
    f.render_widget(TuiClear, footer_area); // Clear the footer area, so no artifacts remain

    let mut footer_spans = Vec::new();
    app.button_areas.clear();

    match app.input_mode {
        InputMode::Normal => {
            let mut buttons = vec![
                ("q", "quit", "quit"),
                ("a", "add server", "add"),
                ("d", "delete server", "delete"),
            ];

            if app.show_help {
                buttons.push(("h", "close help", "help"));
            } else {
                buttons.push(("h", "help", "help"));
            }

            let mut current_x = footer_area.x + 1;
            for (key, label, id) in buttons {
                let text = format!(" {}: {} ", key, label);
                let width = text.chars().count() as u16;
                
                app.button_areas.insert(id.to_string(), Rect {
                    x: current_x,
                    y: footer_area.y + 1,
                    width,
                    height: 1,
                });

                footer_spans.push(Span::styled(
                    text,
                    Style::default().bg(Color::Blue).fg(Color::White),
                ));
                footer_spans.push(Span::raw(" "));
                current_x += width + 1;
            }

            if !app.show_help {
                let help_text = match app.focus {
                    Focus::Endpoints => "| Down/Up: navigate server | Right: focus fields",
                    Focus::Fields => "| Down/Up: navigate fields | Left: focus server | Enter: edit",
                };
                footer_spans.push(Span::raw(help_text));
            }
        }
        InputMode::Editing => {
            let buttons = vec![
                ("Enter", "save", "enter"),
                ("Esc", "cancel edit", "cancel"),
                ("Ctrl-k", "clear", "clear"),
            ];

            let mut current_x = footer_area.x + 1;
            for (key, label, id) in buttons {
                let text = format!(" {}: {} ", key, label);
                let width = text.chars().count() as u16;

                app.button_areas.insert(id.to_string(), Rect {
                    x: current_x,
                    y: footer_area.y + 1,
                    width,
                    height: 1,
                });

                footer_spans.push(Span::styled(
                    text,
                    Style::default().bg(Color::Blue).fg(Color::White),
                ));
                footer_spans.push(Span::raw(" "));
                current_x += width + 1;
            }
            footer_spans.push(Span::raw("| Up/Down/Left: save & move"));
        }
        InputMode::ConfirmDelete => {
            footer_spans.push(Span::raw("Are you sure you want to delete this endpoint? "));
            
            let mut current_x = footer_area.x + 1 + "Are you sure you want to delete this endpoint? ".chars().count() as u16;
            
            let buttons = vec![
                ("y", "Yes", "confirm_yes"),
                ("n", "No", "confirm_no"),
            ];

            for (key, label, id) in buttons {
                let text = format!(" {}: {} ", key, label);
                let width = text.chars().count() as u16;

                app.button_areas.insert(id.to_string(), Rect {
                    x: current_x,
                    y: footer_area.y + 1,
                    width,
                    height: 1,
                });

                footer_spans.push(Span::styled(
                    text,
                    Style::default().bg(Color::Red).fg(Color::White),
                ));
                footer_spans.push(Span::raw(" "));
                current_x += width + 1;
            }
        }
        InputMode::HttpWarning => {
            footer_spans.push(Span::styled("WARNING: Using HTTP is insecure! ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)));
            footer_spans.push(Span::raw("Press Enter/Esc to acknowledge."));
        }
    }

    let footer = Paragraph::new(Line::from(footer_spans))
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, footer_area);

    if let InputMode::HttpWarning = app.input_mode {
        let area = centered_rect(60, 20, f.area());
        f.render_widget(TuiClear, area); //this clears out the background
        let warning_text = vec![
            Line::from(vec![Span::styled("Insecure Protocol Warning", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))]),
            Line::from(""),
            Line::from("The endpoint you specified uses the HTTP protocol."),
            Line::from("This is insecure and can expose your credentials."),
            Line::from(""),
            Line::from("It is strongly recommended to use HTTPS instead."),
            Line::from(""),
            Line::from(vec![Span::styled("Press Enter, Esc or Space to acknowledge", Style::default().add_modifier(Modifier::ITALIC))]),
        ];
        let block = Block::default()
            .title(" Security Risk ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Red));
        let p = Paragraph::new(warning_text)
            .block(block)
            .alignment(ratatui::layout::Alignment::Center);
        f.render_widget(p, area);
    }
}

fn is_http(config: &EndpointConfig) -> bool {
    matches!(util_layer::url_parser::validate_s3_endpoint(&config.s3_url), util_layer::url_parser::S3EndpointType::HTTP)
}

/// helper function to create a centered rect using up certain percentage of the available rect `r`
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Percentage((100 - percent_y) / 2),
                Constraint::Percentage(percent_y),
                Constraint::Percentage((100 - percent_y) / 2),
            ]
            .as_ref(),
        )
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            [
                Constraint::Percentage((100 - percent_x) / 2),
                Constraint::Percentage(percent_x),
                Constraint::Percentage((100 - percent_x) / 2),
            ]
            .as_ref(),
        )
        .split(popup_layout[1])[1]
}
