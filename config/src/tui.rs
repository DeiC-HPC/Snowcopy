use crate::{load_config, save_config, AppConfig, Endpoint, EndpointConfig};
use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Terminal,
};
use std::io;
use crossterm::terminal::{Clear, ClearType};

pub fn run() -> Result<()> {
    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, Clear(ClearType::All), EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    // create app and run it
    let res = run_app(&mut terminal);

    terminal.clear()?;
    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
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
}

pub(crate) enum Focus {
    Endpoints,
    Fields,
}

pub(crate) enum Field {
    Name,
    AccessKey,
    SecretKey,
    Url,
    Region,
}

pub(crate) struct App {
    pub(crate) config: AppConfig,
    pub(crate) list_state: ListState,
    pub(crate) input_mode: InputMode,
    pub(crate) focus: Focus,
    pub(crate) selected_field: Field,
    pub(crate) edit_buffer: String,
    pub(crate) show_help: bool,
}

impl App {
    pub(crate) fn new(config: AppConfig) -> App {
        let mut list_state = ListState::default();
        if !config.endpoints.is_empty() {
            list_state.select(Some(0));
        }
        App {
            config,
            list_state,
            input_mode: InputMode::Normal,
            focus: Focus::Endpoints,
            selected_field: Field::Name,
            edit_buffer: String::new(),
            show_help: false,
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
        let new_endpoint = Endpoint {
            name: "name".to_string(),
            config: Some(EndpointConfig::default()),
            settings: None,
        };
        self.config.endpoints.push(new_endpoint);
        self.list_state.select(Some(self.config.endpoints.len() - 1));
        save_config(&self.config)
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
                save_config(&self.config)?;
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
                Field::Region => endpoint.config.as_ref().and_then(|c| c.s3_region.clone()).unwrap_or_default(),
            };
            self.input_mode = InputMode::Editing;
        }
    }

    pub(crate) fn save_edit(&mut self) -> Result<()> {
        if let Some(i) = self.list_state.selected() {
            match self.selected_field {
                Field::Name => {
                    let new_name = self.edit_buffer.trim().to_string();
                    if new_name.is_empty() {
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
                    endpoint.config.as_mut().unwrap().s3_access_key = self.edit_buffer.clone();
                }
                Field::SecretKey => {
                    let endpoint = &mut self.config.endpoints[i];
                    if endpoint.config.is_none() { endpoint.config = Some(EndpointConfig::default()); }
                    endpoint.config.as_mut().unwrap().s3_secret_key = self.edit_buffer.clone();
                }
                Field::Url => {
                    let endpoint = &mut self.config.endpoints[i];
                    if endpoint.config.is_none() { endpoint.config = Some(EndpointConfig::default()); }
                    endpoint.config.as_mut().unwrap().s3_url = self.edit_buffer.clone();
                }
                Field::Region => {
                    let endpoint = &mut self.config.endpoints[i];
                    if endpoint.config.is_none() { endpoint.config = Some(EndpointConfig::default()); }
                    endpoint.config.as_mut().unwrap().s3_region = if self.edit_buffer.is_empty() { None } else { Some(self.edit_buffer.clone()) };
                }
            }
        }
        self.input_mode = InputMode::Normal;
        save_config(&self.config)
    }
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>) -> Result<()> 
where
    B::Error: std::error::Error + Send + Sync + 'static,
{
    let config = load_config().unwrap_or_default();
    let mut app = App::new(config);

    if app.config.endpoints.is_empty() {
        app.add_endpoint()?;
        app.focus = Focus::Fields;
        app.selected_field = Field::Name;
        app.edit_buffer = "name".to_string();
        app.input_mode = InputMode::Editing;
    }

    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        if let Event::Key(key) = event::read()? {
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
                        app.edit_buffer = "name".to_string();
                        app.input_mode = InputMode::Editing;
                    }
                    KeyCode::Char('d') => {
                        app.delete_endpoint()?;
                    }
                    KeyCode::Char('h') => app.show_help = !app.show_help,
                    KeyCode::Down => {
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
                    KeyCode::Up => {
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
                    KeyCode::Down => {
                        app.save_edit()?;
                        app.next_field();
                        app.start_editing();
                    }
                    KeyCode::Up => {
                        app.save_edit()?;
                        app.previous_field();
                        app.start_editing();
                    }
                    KeyCode::Char('k') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                        app.edit_buffer.clear();
                    }
                    KeyCode::Char(c) => {
                        app.edit_buffer.push(c);
                    }
                    KeyCode::Backspace => {
                        app.edit_buffer.pop();
                    }
                    _ => {}
                },
            }
        }
    }
}

fn ui(f: &mut ratatui::Frame, app: &mut App) {
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
        let help_text = "
Navigation:
  - Up/Down: Navigate endpoints (left) or fields (right)
  - Left/Right: Switch focus between endpoints and fields
  - Enter: Edit selected field
  - Esc: Cancel editing
  - Ctrl-k: Clear field (in edit mode)
  - a: Add new endpoint
  - d: Delete selected endpoint
  - q: Quit
  - h: Toggle help
";
        let help = Paragraph::new(help_text)
            .block(Block::default().title("Help").borders(Borders::ALL));
        f.render_widget(help, chunks[1]);
    } else {
        let body_chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(30), Constraint::Percentage(70)].as_ref())
            .split(chunks[1]);

    // Endpoints List
    let items: Vec<ListItem> = app
        .config
        .endpoints
        .iter()
        .enumerate()
        .map(|(idx, e)| {
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
            }
            ListItem::new(label).style(style)
        })
        .collect();
    let endpoint_list_style = match app.focus {
        Focus::Endpoints => Style::default()
            .bg(Color::LightBlue)
            .fg(Color::Black)
            .add_modifier(Modifier::BOLD),
        Focus::Fields => Style::default()
            .bg(Color::White)
            .fg(Color::White),
    };

    let list = List::new(items)
        .block(Block::default().title("Endpoints").borders(Borders::ALL))
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
                    ]
                    .as_ref(),
                )
                .split(body_chunks[1]);

            let fields = [
                (Field::Name, "Name", endpoint.name.as_str()),
                (Field::AccessKey, "S3 Access Key", endpoint.config.as_ref().map(|c| c.s3_access_key.as_str()).unwrap_or("")),
                (Field::SecretKey, "S3 Secret Key", endpoint.config.as_ref().map(|c| c.s3_secret_key.as_str()).unwrap_or("")),
                (Field::Url, "S3 URL", endpoint.config.as_ref().map(|c| c.s3_url.as_str()).unwrap_or("")),
                (Field::Region, "S3 Region (non-aws s3's endpoints commonly use us-east-1)", endpoint.config.as_ref().and_then(|c| c.s3_region.as_deref()).unwrap_or("")),
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
                        InputMode::Normal | InputMode::ConfirmDelete => *value,
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
                } else if is_incomplete {
                    style = style.fg(Color::Yellow);
                }

                let mut block = Block::default().title(*label).borders(Borders::ALL);
                if is_duplicate_name {
                    block = block.title("Duplicate!");
                } else if is_incomplete {
                    block = block.title("⚠");
                }

                let p = Paragraph::new(display_value)
                    .style(style)
                    .block(block);
                f.render_widget(p, details_chunks[idx]);

                if is_selected {
                    if let InputMode::Editing = app.input_mode {
                        f.set_cursor_position((
                            details_chunks[idx].x + display_value.len() as u16 + 1,
                            details_chunks[idx].y + 1,
                        ));
                    }
                }
            }
        }
    }

    let footer_text = match app.input_mode {
        InputMode::Normal => match app.focus {
            Focus::Endpoints => "q: quit | a: add | d: delete | Down/Up: navigate endpoints | Right: focus fields | h: help",
            Focus::Fields => "q: quit | a: add | d: delete | Down/Up: navigate fields | Left: focus endpoints | Enter: edit | h: help",
        },
        InputMode::Editing => "Enter: save | Esc: cancel | Up/Down/Left: save & move | Ctrl-k: clear",
        InputMode::ConfirmDelete => "Are you sure you want to delete this endpoint? (y/n)",
    };
    let footer = Paragraph::new(footer_text)
        .block(Block::default().borders(Borders::ALL));
    f.render_widget(footer, chunks[2]);
    }
}
