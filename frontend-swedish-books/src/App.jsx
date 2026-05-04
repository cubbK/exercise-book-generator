import { createContext, useContext, useEffect, useState } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  Link,
  useParams,
  useNavigate,
} from "react-router-dom";
import booksData from "../books_extract.json";
import {
  Box,
  Button,
  Container,
  CssBaseline,
  Divider,
  FormControl,
  InputLabel,
  List,
  ListItem,
  ListItemButton,
  ListItemText,
  MenuItem,
  Select,
  Tab,
  Tabs,
  ThemeProvider,
  Typography,
  createTheme,
} from "@mui/material";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import ArrowForwardIcon from "@mui/icons-material/ArrowForward";

const LanguageContext = createContext({ mode: "easy", setMode: () => {} });

const solarized = {
  base3: "#FDF6E3",
  base2: "#EEE8D5",
  base1: "#93A1A1",
  base00: "#657B83",
  base01: "#586E75",
  base02: "#073642",
  yellow: "#B58900",
};

const theme = createTheme({
  palette: {
    background: { default: solarized.base3, paper: solarized.base2 },
    text: { primary: solarized.base01, secondary: solarized.base1 },
    primary: { main: solarized.yellow },
    divider: solarized.base1,
  },
  typography: {
    fontFamily: '"Inter", "Helvetica Neue", Arial, sans-serif',
    fontSize: 18,
    body1: { lineHeight: 1.85, fontSize: "1.1rem", color: solarized.base01 },
    h4: { color: solarized.base02, fontWeight: 700 },
    h5: { color: solarized.base02, fontWeight: 600 },
    h6: { color: solarized.base00, fontWeight: 600 },
  },
  components: {
    MuiAccordion: {
      defaultProps: {
        TransitionProps: { timeout: 100 },
      },
      styleOverrides: {
        root: {
          background: "transparent",
          boxShadow: "none",
          border: "none",
          borderTop: `1px solid ${solarized.base2}`,
          "&:before": { display: "none" },
        },
      },
    },
    MuiAccordionSummary: {
      styleOverrides: {
        root: {
          color: solarized.base1,
          minHeight: 32,
          padding: "0 4px",
          "& .MuiAccordionSummary-content": { margin: "4px 0" },
        },
      },
    },
    MuiAccordionDetails: {
      styleOverrides: {
        root: { padding: "8px 4px 12px" },
      },
    },
    MuiListItemButton: {
      styleOverrides: {
        root: {
          borderRadius: 4,
          "&:hover": { background: solarized.base2 },
        },
      },
    },
  },
});

function TextBlock({ text }) {
  if (!text) return <Typography color="text.secondary">No content.</Typography>;
  return (
    <Typography
      component="pre"
      sx={{
        whiteSpace: "pre-wrap",
        fontFamily: '"Inter", "Helvetica Neue", Arial, sans-serif',
        fontSize: "1.1rem",
        lineHeight: 1.85,
        color: solarized.base01,
        letterSpacing: "0.01em",
      }}
    >
      {text}
    </Typography>
  );
}

const CHAPTER_TABS = [
  { label: "Summary", key: "a2_summary" },
  { label: "Original", key: "raw_text" },
  { label: "A2 Swedish", key: "a2_text" },
  { label: "B1-B2 Swedish", key: "b1b2_text" },
  { label: "Normal Swedish", key: "swedish_text" },
];

function Chapter({ chapter }) {
  const { mode } = useContext(LanguageContext);
  const [tab, setTab] = useState(null);

  const mainText =
    mode === "easy"
      ? chapter.a2_text || chapter.raw_text
      : mode === "b1b2"
        ? chapter.b1b2_text || chapter.raw_text
        : chapter.swedish_text || chapter.raw_text;

  return (
    <Box
      sx={{
        mb: 4,
        border: `1px solid ${solarized.base1}`,
        borderRadius: 2,
        p: 3,
        background: solarized.base3,
      }}
    >
      <TextBlock text={mainText} />

      <Box sx={{ mt: 3, borderTop: `1px solid ${solarized.base2}` }}>
        <Tabs
          value={tab ?? false}
          onChange={(_, v) => setTab(tab === v ? null : v)}
          variant="scrollable"
          scrollButtons="auto"
          textColor="inherit"
          TabIndicatorProps={{
            style: { background: solarized.yellow },
          }}
          sx={{
            minHeight: 36,
            "& .MuiTab-root": {
              minHeight: 36,
              fontSize: "0.7rem",
              letterSpacing: "0.05em",
              textTransform: "uppercase",
              color: solarized.base1,
              padding: "6px 12px",
            },
            "& .Mui-selected": { color: solarized.yellow },
          }}
        >
          {CHAPTER_TABS.map((t) => (
            <Tab key={t.key} label={t.label} value={t.key} />
          ))}
        </Tabs>
        {tab !== null && (
          <Box sx={{ pt: 2 }}>
            <TextBlock text={chapter[tab]} />
          </Box>
        )}
      </Box>
    </Box>
  );
}

function buildBookMap() {
  const bookMap = {};
  for (const ch of booksData) {
    if (!bookMap[ch.book_id]) {
      bookMap[ch.book_id] = { title: ch.book_title, chapters: [] };
    }
    bookMap[ch.book_id].chapters.push(ch);
  }
  for (const book of Object.values(bookMap)) {
    book.chapters.sort(
      (a, b) => Number(a.chapter_order) - Number(b.chapter_order),
    );
  }
  return bookMap;
}

const bookMap = buildBookMap();

const PROGRESS_KEY = "bookProgress";

function saveProgress(bookId, chapterOrder) {
  const all = loadProgress();
  all[bookId] = String(chapterOrder);
  localStorage.setItem(PROGRESS_KEY, JSON.stringify(all));
}

function loadProgress() {
  try {
    return JSON.parse(localStorage.getItem(PROGRESS_KEY)) || {};
  } catch {
    return {};
  }
}

function BookListPage() {
  const progress = loadProgress();

  return (
    <>
      <LanguageSelector />
      <Container maxWidth="sm" sx={{ py: 4 }}>
        <Typography variant="h4" gutterBottom>
          Swedish Books
        </Typography>
        <Divider sx={{ mb: 2 }} />
        <List>
          {Object.entries(bookMap).map(([bookId, book]) => {
            const savedOrder = progress[bookId];
            const savedChapter = savedOrder
              ? book.chapters.find(
                  (ch) => String(ch.chapter_order) === savedOrder,
                )
              : null;
            return (
              <ListItem
                key={bookId}
                disablePadding
                secondaryAction={
                  savedChapter ? (
                    <Button
                      component={Link}
                      to={`/book/${bookId}/chapter/${savedOrder}`}
                      size="small"
                      variant="outlined"
                      sx={{ whiteSpace: "nowrap" }}
                    >
                      Resume
                    </Button>
                  ) : null
                }
              >
                <ListItemButton
                  component={Link}
                  to={`/book/${bookId}`}
                  sx={{ pr: savedChapter ? 14 : 2 }}
                >
                  <ListItemText
                    primary={book.title}
                    secondary={
                      savedChapter
                        ? `Last: ${savedChapter.chapter_title || `Chapter ${savedChapter.chapter_order}`}`
                        : `${book.chapters.length} chapters`
                    }
                  />
                </ListItemButton>
              </ListItem>
            );
          })}
        </List>
      </Container>
    </>
  );
}

function BookPage() {
  const { bookId } = useParams();
  const book = bookMap[bookId];

  if (!book) {
    return (
      <Container maxWidth="sm" sx={{ py: 4 }}>
        <Typography>Book not found.</Typography>
      </Container>
    );
  }

  return (
    <Container maxWidth="sm" sx={{ py: 4 }}>
      <Button
        startIcon={<ArrowBackIcon />}
        component={Link}
        to="/"
        sx={{ mb: 2 }}
      >
        All Books
      </Button>
      <Typography variant="h4" gutterBottom>
        {book.title}
      </Typography>
      <Divider sx={{ mb: 2 }} />
      <List>
        {book.chapters.map((ch) => (
          <ListItemButton
            key={ch.chapter_id}
            component={Link}
            to={`/book/${bookId}/chapter/${ch.chapter_order}`}
          >
            <ListItemText
              primary={ch.chapter_title || `Chapter ${ch.chapter_order}`}
            />
          </ListItemButton>
        ))}
      </List>
    </Container>
  );
}

function ChapterPage() {
  const { bookId, chapterOrder } = useParams();
  const navigate = useNavigate();
  const book = bookMap[bookId];

  const idx = book
    ? book.chapters.findIndex(
        (ch) => String(ch.chapter_order) === String(chapterOrder),
      )
    : -1;
  const chapter = book ? book.chapters[idx] : null;
  const prev = book ? book.chapters[idx - 1] : null;
  const next = book ? book.chapters[idx + 1] : null;

  useEffect(() => {
    if (chapter) {
      saveProgress(bookId, chapterOrder);
    }
  }, [bookId, chapterOrder, chapter]);

  if (!book) {
    return (
      <Container maxWidth="sm" sx={{ py: 4 }}>
        <Typography>Book not found.</Typography>
      </Container>
    );
  }

  if (!chapter) {
    return (
      <Container maxWidth="sm" sx={{ py: 4 }}>
        <Typography>Chapter not found.</Typography>
      </Container>
    );
  }

  return (
    <Container maxWidth="sm" sx={{ py: 4 }}>
      <Box sx={{ display: "flex", alignItems: "center", gap: 1, mb: 2 }}>
        <Button
          startIcon={<ArrowBackIcon />}
          component={Link}
          to={`/book/${bookId}`}
          size="small"
        >
          {book.title}
        </Button>
      </Box>

      <Typography variant="h5" gutterBottom>
        {chapter.chapter_title || `Chapter ${chapter.chapter_order}`}
      </Typography>
      <Divider sx={{ mb: 3 }} />

      <Chapter chapter={chapter} />

      <Box sx={{ display: "flex", justifyContent: "space-between", mt: 3 }}>
        <Button
          startIcon={<ArrowBackIcon />}
          disabled={!prev}
          onClick={() =>
            prev && navigate(`/book/${bookId}/chapter/${prev.chapter_order}`)
          }
        >
          {prev ? prev.chapter_title || `Chapter ${prev.chapter_order}` : ""}
        </Button>
        <Button
          endIcon={<ArrowForwardIcon />}
          disabled={!next}
          onClick={() =>
            next && navigate(`/book/${bookId}/chapter/${next.chapter_order}`)
          }
        >
          {next ? next.chapter_title || `Chapter ${next.chapter_order}` : ""}
        </Button>
      </Box>
    </Container>
  );
}

function LanguageSelector() {
  const { mode, setMode } = useContext(LanguageContext);
  return (
    <Box
      sx={{
        background: solarized.base2,
        borderBottom: `1px solid ${solarized.base1}`,
        px: 2,
        py: 1,
        display: "flex",
        justifyContent: "flex-end",
      }}
    >
      <FormControl size="small" sx={{ minWidth: 180 }}>
        <InputLabel id="lang-select-label">Swedish mode</InputLabel>
        <Select
          labelId="lang-select-label"
          value={mode}
          label="Swedish mode"
          onChange={(e) => setMode(e.target.value)}
        >
          <MenuItem value="easy">Easy Swedish (A2)</MenuItem>
          <MenuItem value="b1b2">B1-B2 Swedish</MenuItem>
          <MenuItem value="normal">Normal Swedish</MenuItem>
        </Select>
      </FormControl>
    </Box>
  );
}

function App() {
  const [mode, setMode] = useState(() => {
    try {
      return localStorage.getItem("langMode") || "easy";
    } catch {
      return "easy";
    }
  });

  const handleSetMode = (value) => {
    setMode(value);
    try {
      localStorage.setItem("langMode", value);
    } catch {
      // ignore
    }
  };

  return (
    <LanguageContext.Provider value={{ mode, setMode: handleSetMode }}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<BookListPage />} />
            <Route path="/book/:bookId" element={<BookPage />} />
            <Route
              path="/book/:bookId/chapter/:chapterOrder"
              element={<ChapterPage />}
            />
          </Routes>
        </BrowserRouter>
      </ThemeProvider>
    </LanguageContext.Provider>
  );
}

export default App;
