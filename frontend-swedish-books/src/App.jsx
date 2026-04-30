import { useState } from "react";
import booksData from "../books_extract.json";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Button,
  Container,
  CssBaseline,
  Divider,
  List,
  ListItemButton,
  ListItemText,
  ThemeProvider,
  Typography,
  createTheme,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";

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

function Chapter({ chapter }) {
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
      <TextBlock text={chapter.a2_text || chapter.raw_text} />

      <Box sx={{ mt: 3, borderTop: `1px solid ${solarized.base2}` }}>
        <Accordion>
          <AccordionSummary
            expandIcon={<ExpandMoreIcon sx={{ fontSize: 16 }} />}
          >
            <Typography
              variant="caption"
              sx={{
                color: solarized.base1,
                letterSpacing: "0.05em",
                textTransform: "uppercase",
              }}
            >
              Summary
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <TextBlock text={chapter.a2_summary} />
          </AccordionDetails>
        </Accordion>

        <Accordion>
          <AccordionSummary
            expandIcon={<ExpandMoreIcon sx={{ fontSize: 16 }} />}
          >
            <Typography
              variant="caption"
              sx={{
                color: solarized.base1,
                letterSpacing: "0.05em",
                textTransform: "uppercase",
              }}
            >
              Original
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <TextBlock text={chapter.raw_text} />
          </AccordionDetails>
        </Accordion>

        <Accordion>
          <AccordionSummary
            expandIcon={<ExpandMoreIcon sx={{ fontSize: 16 }} />}
          >
            <Typography
              variant="caption"
              sx={{
                color: solarized.base1,
                letterSpacing: "0.05em",
                textTransform: "uppercase",
              }}
            >
              Normal Swedish
            </Typography>
          </AccordionSummary>
          <AccordionDetails>
            <TextBlock text={chapter.swedish_text} />
          </AccordionDetails>
        </Accordion>
      </Box>
    </Box>
  );
}

function App() {
  const [selectedBookId, setSelectedBookId] = useState(null);

  const chapters = booksData;

  const bookMap = {};
  for (const ch of chapters) {
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

  if (selectedBookId) {
    const book = bookMap[selectedBookId];
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Container maxWidth="sm" sx={{ py: 4 }}>
          <Button
            startIcon={<ArrowBackIcon />}
            onClick={() => setSelectedBookId(null)}
            sx={{ mb: 2 }}
          >
            All Books
          </Button>
          <Typography variant="h4" gutterBottom>
            {book.title}
          </Typography>
          <Divider sx={{ mb: 3 }} />
          {book.chapters.map((ch) => (
            <Chapter key={ch.chapter_id} chapter={ch} />
          ))}
        </Container>
      </ThemeProvider>
    );
  }

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <Container maxWidth="sm" sx={{ py: 4 }}>
        <Typography variant="h4" gutterBottom>
          Swedish Books
        </Typography>
        <Divider sx={{ mb: 2 }} />
        <List>
          {Object.entries(bookMap).map(([bookId, book]) => (
            <ListItemButton
              key={bookId}
              onClick={() => setSelectedBookId(bookId)}
            >
              <ListItemText
                primary={book.title}
                secondary={`${book.chapters.length} chapters`}
              />
            </ListItemButton>
          ))}
        </List>
      </Container>
    </ThemeProvider>
  );
}

export default App;
