"""Simple GUI orchestrating the VTB report import and analysis pipeline."""

from __future__ import annotations

import contextlib
import importlib.util
import logging
import queue
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText

ROOT_DIR = Path(__file__).resolve().parent
READ_SCRIPT = ROOT_DIR / "ReadOT4ET.py"
HTTP_SCRIPT = ROOT_DIR / "HTTP-Req_PORTFOLIO.py"
ANALIZ_SCRIPT = ROOT_DIR / "ANALIZ_VTB.py"
DEFAULT_PDF_FILENAME = "VTB_Report.pdf"


def _load_module(module_name: str, script_path: Path):
    if not script_path.exists():
        raise FileNotFoundError(f"Cannot find script: {script_path}")
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


READ_MODULE = _load_module("vtb_read_module", READ_SCRIPT)
HTTP_MODULE = _load_module("vtb_http_module", HTTP_SCRIPT)
ANALIZ_MODULE = _load_module("vtb_analiz_module", ANALIZ_SCRIPT)


def _default_pdf_directory() -> Path:
    """Try to reuse the desktop path the user requested, fall back to HOME/ROOT."""

    candidate = Path.home() / "OneDrive" / "Рабочий стол"
    if candidate.exists():
        return candidate
    home = Path.home()
    if home.exists():
        return home
    return ROOT_DIR


class _QueueWriter:
    """File-like object that forwards writes to a queue."""

    def __init__(self, target_queue: queue.Queue[str]):
        self.queue = target_queue

    def write(self, message: str) -> None:
        if message:
            self.queue.put(message)

    def flush(self) -> None:  # pragma: no cover - required for file-like API
        pass


class _QueueLoggingHandler(logging.Handler):
    """Logging handler that mirrors log records into the GUI log queue."""

    def __init__(self, target_queue: queue.Queue[str]):
        super().__init__()
        self.queue = target_queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:  # pragma: no cover - fallback mirrors std logging
            self.handleError(record)
            return
        self.queue.put(msg + "\n")


@contextlib.contextmanager
def redirect_output(target_queue: queue.Queue[str]):
    """Redirect stdout/stderr and logging records into the GUI queue."""

    writer = _QueueWriter(target_queue)
    original_stdout, original_stderr = sys.stdout, sys.stderr
    sys.stdout = writer
    sys.stderr = writer

    logging_handler = _QueueLoggingHandler(target_queue)
    logging_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    root_logger = logging.getLogger()
    root_logger.addHandler(logging_handler)
    try:
        yield
    finally:
        root_logger.removeHandler(logging_handler)
        sys.stdout = original_stdout
        sys.stderr = original_stderr


def run_import(path: Path) -> None:
    """Run ReadOT4ET pipeline for a single file."""

    if not path.is_file():
        raise FileNotFoundError(f"Selected file does not exist: {path}")

    READ_MODULE.ensure_dirs()
    conn = READ_MODULE.connect_db()
    READ_MODULE.init_db(conn)
    try:
        total_rows, inserted_rows = READ_MODULE.process_file(str(path), conn)
    finally:
        conn.close()

    print(
        f"Импорт завершён. Обработано строк: {total_rows}, "
        f"добавлено в базу: {inserted_rows}"
    )


def run_http_pipeline() -> None:
    """Run HTTP data enrichment pipeline."""

    HTTP_MODULE.main()


def run_analysis() -> list[str]:
    """Run final analytics pipeline and return the prepared messages."""

    return ANALIZ_MODULE.run_analysis_pipeline()


class VTBApp(tk.Tk):
    """Tkinter application that orchestrates the workflow."""

    def __init__(self) -> None:
        super().__init__()
        self.title("VTB Portfolio Assistant")
        self.geometry("960x640")

        self.file_var = tk.StringVar()
        self.file_var.trace_add("write", self._on_file_change)

        self.log_queue: queue.Queue[str] = queue.Queue()
        self._is_running = False
        self.last_messages: list[str] | None = None
        self._pending_success_message = ""

        self._build_ui()
        self._process_log_queue()

    def _build_ui(self) -> None:
        top_frame = tk.Frame(self)
        top_frame.pack(fill="x", padx=10, pady=10)

        tk.Label(top_frame, text="Файл отчёта:").pack(side="left")
        self.file_entry = tk.Entry(top_frame, textvariable=self.file_var, width=80)
        self.file_entry.pack(side="left", padx=(5, 5), expand=True, fill="x")

        tk.Button(top_frame, text="Обзор...", command=self._select_file).pack(side="left")

        buttons_frame = tk.Frame(self)
        buttons_frame.pack(fill="x", padx=10, pady=(0, 10))

        self.btn_import_all = tk.Button(
            buttons_frame,
            text="Импорт и анализ",
            command=lambda: self._start_pipeline(include_import=True),
            state="disabled",
            width=20,
        )
        self.btn_import_all.pack(side="left", padx=(0, 10))

        self.btn_analyze = tk.Button(
            buttons_frame,
            text="Анализ",
            command=lambda: self._start_pipeline(include_import=False),
            width=20,
        )
        self.btn_analyze.pack(side="left")

        log_frame = tk.LabelFrame(self, text="Журнал выполнения")
        log_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.log_text = ScrolledText(log_frame, state="disabled", wrap="word")
        self.log_text.pack(fill="both", expand=True)

        actions_frame = tk.Frame(self)
        actions_frame.pack(fill="x", padx=10, pady=(0, 10))

        self.btn_pdf_tg = tk.Button(
            actions_frame,
            text="PDF+TG",
            state="disabled",
            width=12,
            command=self._handle_pdf_and_tg,
        )
        self.btn_pdf_tg.pack(side="left", padx=(0, 10))

        self.btn_tg = tk.Button(
            actions_frame,
            text="TG",
            state="disabled",
            width=12,
            command=self._handle_tg,
        )
        self.btn_tg.pack(side="left", padx=(0, 10))

        self.btn_pdf = tk.Button(
            actions_frame,
            text="PDF",
            state="disabled",
            width=12,
            command=self._handle_pdf,
        )
        self.btn_pdf.pack(side="left")

        self._update_share_buttons()

    def _update_share_buttons(self) -> None:
        state = "normal" if (self.last_messages and not self._is_running) else "disabled"
        for button in getattr(self, "btn_pdf_tg", None), getattr(self, "btn_tg", None), getattr(self, "btn_pdf", None):
            if button is not None:
                button.config(state=state)

    def _ensure_report_ready(self) -> bool:
        if self.last_messages:
            return True
        messagebox.showwarning("Нет отчёта", "Сначала выполните анализ, чтобы сформировать отчёт.")
        return False

    def _ask_pdf_path(self) -> Path | None:
        initial_dir = _default_pdf_directory()
        file_path = filedialog.asksaveasfilename(
            title="Экспорт PDF-отчёта",
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf")],
            initialdir=str(initial_dir),
            initialfile=DEFAULT_PDF_FILENAME,
        )
        return Path(file_path) if file_path else None

    def _handle_pdf_and_tg(self) -> None:
        if not self._ensure_report_ready():
            return
        pdf_path = self._ask_pdf_path()
        if not pdf_path:
            return
        messages = list(self.last_messages or [])
        self._pending_success_message = "PDF сохранён и отчёт отправлен в Telegram."

        def worker() -> None:
            print(f"Сохранение PDF в {pdf_path}")
            ANALIZ_MODULE.export_report_pdf(messages, pdf_path)
            print("PDF сохранён. Отправка отчёта в Telegram...")
            ANALIZ_MODULE.send_telegram_messages(messages)
            print("Отправка в Telegram завершена.")

        self._start_aux_task(worker)

    def _handle_tg(self) -> None:
        if not self._ensure_report_ready():
            return
        messages = list(self.last_messages or [])
        self._pending_success_message = "Отправка в Telegram завершена."

        def worker() -> None:
            print("Отправка в Telegram...")
            ANALIZ_MODULE.send_telegram_messages(messages)
            print("Отправка в Telegram завершена.")

        self._start_aux_task(worker)

    def _handle_pdf(self) -> None:
        if not self._ensure_report_ready():
            return
        pdf_path = self._ask_pdf_path()
        if not pdf_path:
            return
        messages = list(self.last_messages or [])
        self._pending_success_message = "PDF сохранён."

        def worker() -> None:
            print(f"Сохранение PDF в {pdf_path}")
            ANALIZ_MODULE.export_report_pdf(messages, pdf_path)
            print("PDF сохранён.")

        self._start_aux_task(worker)

    def _select_file(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Выберите отчёт VTB",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
        )
        if file_path:
            self.file_var.set(file_path)

    def _on_file_change(self, *_args) -> None:
        if self.file_var.get().strip():
            self.btn_import_all.config(state="normal")
        else:
            self.btn_import_all.config(state="disabled")

    def _start_aux_task(self, worker) -> None:
        if self._is_running:
            return
        self._set_running(True)
        thread = threading.Thread(target=self._run_aux_task, args=(worker,), daemon=True)
        thread.start()

    def _run_aux_task(self, worker) -> None:
        error_message: str | None = None
        try:
            with redirect_output(self.log_queue):
                worker()
        except Exception as exc:  # noqa: BLE001
            error_message = str(exc)
            self.log_queue.put(f"\nОшибка выполнения: {error_message}\n")
        finally:
            self.after(0, lambda: self._finish_task(error_message))

    def _start_pipeline(self, include_import: bool) -> None:
        if self._is_running:
            return

        selected_file: Path | None = None
        if include_import:
            path_text = self.file_var.get().strip()
            if not path_text:
                messagebox.showwarning("Файл не выбран", "Пожалуйста, выберите файл для импорта.")
                return
            selected_file = Path(path_text)
            if not selected_file.is_file():
                messagebox.showerror("Файл не найден", f"Не удалось найти файл: {selected_file}")
                return

        self.last_messages = None
        self._update_share_buttons()
        self._pending_success_message = "Пайплайн успешно завершён."
        self._set_running(True)
        thread = threading.Thread(
            target=self._run_pipeline,
            args=(include_import, selected_file),
            daemon=True,
        )
        thread.start()

    def _run_pipeline(self, include_import: bool, selected_file: Path | None) -> None:
        error_message: str | None = None
        messages: list[str] | None = None
        try:
            with redirect_output(self.log_queue):
                if include_import and selected_file:
                    print(f"Запуск импорта файла: {selected_file}")
                    run_import(selected_file)
                print("Запуск HTTP_Req_PORTFOLIO...")
                run_http_pipeline()
                print("Запуск ANALIZ_VTB...")
                messages = run_analysis()
                print("Все шаги успешно завершены.")
        except Exception as exc:  # noqa: BLE001 - surface exact exception to user
            error_message = str(exc)
            self.log_queue.put(f"\nПроцесс остановлен: {error_message}\n")
        finally:
            self.after(0, lambda: self._on_pipeline_finished(error_message, messages))

    def _on_pipeline_finished(
        self, error_message: str | None, messages: list[str] | None
    ) -> None:
        if not error_message and messages is not None:
            self.last_messages = list(messages)
        self._update_share_buttons()
        self._finish_task(error_message)

    def _set_running(self, running: bool) -> None:
        self._is_running = running
        state = "disabled" if running else "normal"
        self.btn_import_all.config(state=state if self.file_var.get().strip() else "disabled")
        self.btn_analyze.config(state=state)
        self._update_share_buttons()

    def _process_log_queue(self) -> None:
        try:
            while True:
                message = self.log_queue.get_nowait()
                self._append_log(message)
        except queue.Empty:
            pass
        finally:
            self.after(100, self._process_log_queue)

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, message)
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _finish_task(self, error_message: str | None) -> None:
        self._set_running(False)
        if error_message:
            messagebox.showerror("Процесс завершился с ошибкой", error_message)
        elif self._pending_success_message:
            messagebox.showinfo("Готово", self._pending_success_message)
        self._pending_success_message = ""


if __name__ == "__main__":
    app = VTBApp()
    app.mainloop()
