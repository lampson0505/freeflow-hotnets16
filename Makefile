default:
	pdflatex main.tex
full:
	pdflatex main.tex
	bibtex main.tex
	pdflatex main.tex
	pdflatex main.tex

clean:
	rm *.pdf *.log *.aux
