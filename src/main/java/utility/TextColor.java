package utility;

/**
 * The TextColor enumeration.
 * Define multiple colors for the logger messages.
 *
 * @author  Federica Vicini
 * @author  Michele Zenoni
 */
public enum TextColor {
	BLACK("\u001B[30m"),
	RED("\u001B[31m"),
	GREEN("\u001B[32m"),
	YELLOW("\u001B[33m"),
	BLUE("\u001B[34m"),
	PURPLE("\u001B[35m"),
	CYAN("\u001B[36m"),
	WHITE("\u001B[37m"),
	RESET("\u001B[0m");

	private final String ansiCode;

	TextColor(String ansiCode) {
		this.ansiCode = ansiCode;
	}

	private String getAnsiCode() {
		return this.ansiCode;
	}

	public String colorText(String msg) {
		return this.getAnsiCode() + msg + RESET.getAnsiCode();
	}
}
