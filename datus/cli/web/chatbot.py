# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

"""
Web Chatbot server for Datus Agent.

Serves a React-based chatbot frontend (``@datus/web-chatbot`` UMD bundle)
backed by the standard Datus Agent API routes.  Replaces the former
Streamlit-based implementation with a lightweight FastAPI static-file server.
"""

import argparse
import os
import webbrowser

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from datus.api.service import create_app
from datus.utils.loggings import get_logger

logger = get_logger(__name__)

_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")

# Default path to the pre-built @datus/web-chatbot dist directory.
# Override at runtime via the --chatbot-dist CLI flag.
_DEFAULT_CHATBOT_DIST = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "Datus-saas", "packages", "web-chatbot", "dist")
)


def _build_agent_args(args: argparse.Namespace) -> argparse.Namespace:
    """Bridge CLI ``datus web`` arguments to the shape expected by
    ``create_app`` / ``DatusAPIService``.
    """
    agent_args = argparse.Namespace(
        namespace=args.namespace,
        config=getattr(args, "config", None),
        debug=getattr(args, "debug", False),
        # Fields expected by DatusAPIService but not present in CLI args
        max_steps=20,
        workflow="chat_agentic",
        load_cp=None,
        source="web",
        interactive=True,
        output_dir=getattr(args, "output_dir", "./output"),
        log_level="DEBUG" if getattr(args, "debug", False) else "INFO",
    )
    return agent_args


def _read_template() -> str:
    """Read the HTML template from disk (once)."""
    template_path = os.path.join(_TEMPLATES_DIR, "index.html")
    with open(template_path, encoding="utf-8") as f:
        return f.read()


def create_web_app(args: argparse.Namespace) -> FastAPI:
    """Create the FastAPI application that serves both the API and the chatbot
    frontend.

    Parameters
    ----------
    args:
        Parsed CLI arguments from ``datus web``.
    """
    agent_args = _build_agent_args(args)
    app = create_app(agent_args)

    # ── Resolve chatbot dist directory ──────────────────────────────
    chatbot_dist = getattr(args, "chatbot_dist", None) or _DEFAULT_CHATBOT_DIST
    chatbot_dist = os.path.abspath(os.path.expanduser(chatbot_dist))

    if not os.path.isdir(chatbot_dist):
        logger.warning(
            f"Chatbot dist directory not found: {chatbot_dist}. "
            "The frontend assets will not be available. "
            "Build @datus/web-chatbot or pass --chatbot-dist."
        )
    else:
        # Serve the UMD bundle + CSS at /chatbot-assets/
        app.mount(
            "/chatbot-assets",
            StaticFiles(directory=chatbot_dist),
            name="chatbot-assets",
        )
        logger.info(f"Serving chatbot assets from {chatbot_dist}")

    # ── HTML template route ─────────────────────────────────────────
    html_template = _read_template()
    host = getattr(args, "host", "localhost")
    port = getattr(args, "port", 8501)
    request_origin = f"http://{host}:{port}"
    user_name = getattr(args, "user_name", None) or os.getenv("USER", "User")

    # Pre-render the template with config values
    rendered_html = html_template.replace("{{ request_origin }}", request_origin).replace("{{ user_name }}", user_name)

    # Override the default JSON root endpoint with the chatbot page
    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def chatbot_page(request: Request):
        return HTMLResponse(content=rendered_html)

    return app


def run_web_interface(args: argparse.Namespace) -> None:
    """Entry point called by ``datus web``.

    Creates the FastAPI app and starts uvicorn.
    """
    from datus.cli.web.config_manager import get_home_from_config
    from datus.utils.path_manager import set_current_path_manager

    config_path = getattr(args, "config", None) or "conf/agent.yml"
    set_current_path_manager(get_home_from_config(config_path))

    host = getattr(args, "host", "localhost")
    port = getattr(args, "port", 8501)
    url = f"http://{host}:{port}"

    if getattr(args, "subagent", ""):
        url += f"/?subagent={args.subagent}"

    logger.info("Starting Datus Web Interface...")
    logger.info(f"Namespace: {args.namespace}")
    logger.info(f"Server URL: {url}")

    app = create_web_app(args)

    # Open the browser after a short delay
    def _open_browser():
        import threading
        import time

        def _open():
            time.sleep(1.5)
            webbrowser.open(url)

        t = threading.Thread(target=_open, daemon=True)
        t.start()

    _open_browser()

    try:
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level="debug" if getattr(args, "debug", False) else "info",
        )
    except KeyboardInterrupt:
        logger.info("Web server stopped")
