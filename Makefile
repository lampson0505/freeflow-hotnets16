default:
	pdflatex main.tex
	bibtex main
	pdflatex main.tex
	pdflatex main.tex
latex:
	pdflatex main.tex
clean:
	rm *.pdf *.log *.aux
