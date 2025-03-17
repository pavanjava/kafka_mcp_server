import argparse
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def main():
    """
    Main entry point for the mcp-server-kafka script.
    It runs the MCP server with a specific transport protocol.
    """

    # Parse the command-line arguments to determine the transport protocol.
    parser = argparse.ArgumentParser(description="mcp-server-kafka")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
    )
    args = parser.parse_args()

    # Import is done here to make sure environment variables are loaded
    # only after we make the changes.
    from server import mcp

    mcp.run(transport=args.transport)

if __name__ == "__main__":
    main()