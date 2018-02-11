package testhelper

import (
	"bytes"
	"io/ioutil"
	"log"
	"runtime/debug"
	"time"

	"github.com/jeffrom/logd/config"
)

var Golden bool

var SomeLines = [][]byte{
	[]byte("When Marx undertook his critique of the capitalistic mode of production, this mode was in its infancy. Marx directed his efforts in such a way as to give them prognostic value. He went back to the basic conditions underlying capitalistic production and through his presentation showed what could be expected of capitalism in the future. The result was that one could expect it not only to exploit the proletariat with increasing intensity, but ultimately to create conditions which would make it possible to abolish capitalism itself."),
	[]byte("The transformation of the superstructure, which takes place far more slowly than that of the substructure, has taken more than half a century to manifest in all areas of culture the change in the conditions of production. Only today can it be indicated what form this has taken. Certain prognostic requirements should be met by these statements. However, theses about the art of the proletariat after its assumption of power or about the art of a classless society would have less bearing on these demands than theses about the developmental tendencies of art under present conditions of production. Their dialectic is no less noticeable in the superstructure than in the economy. It would therefore be wrong to underestimate the value of such theses as a weapon. They brush aside a number of outmoded concepts, such as creativity and genius, eternal value and mystery – concepts whose uncontrolled (and at present almost uncontrollable) application would lead to a processing of data in the Fascist sense. The concepts which are introduced into the theory of art in what follows differ from the more familiar terms in that they are completely useless for the purposes of Fascism. They are, on the other hand, useful for the formulation of revolutionary demands in the politics of art."),
	[]byte("In principle a work of art has always been reproducible. Man-made artifacts could always be imitated by men. Replicas were made by pupils in practice of their craft, by masters for diffusing their works, and, finally, by third parties in the pursuit of gain. Mechanical reproduction of a work of art, however, represents something new. Historically, it advanced intermittently and in leaps at long intervals, but with accelerated intensity. The Greeks knew only two procedures of technically reproducing works of art: founding and stamping. Bronzes, terra cottas, and coins were the only art works which they could produce in quantity. All others were unique and could not be mechanically reproduced. With the woodcut graphic art became mechanically reproducible for the first time, long before script became reproducible by print. The enormous changes which printing, the mechanical reproduction of writing, has brought about in literature are a familiar story. However, within the phenomenon which we are here examining from the perspective of world history, print is merely a special, though particularly important, case. During the Middle Ages engraving and etching were added to the woodcut; at the beginning of the nineteenth century lithography made its appearance. With lithography the technique of reproduction reached an essentially new stage. This much more direct process was distinguished by the tracing of the design on a stone rather than its incision on a block of wood or its etching on a copperplate and permitted graphic art for the first time to put its products on the market, not only in large numbers as hitherto, but also in daily changing forms. Lithography enabled graphic art to illustrate everyday life, and it began to keep pace with printing. But only a few decades after its invention, lithography was surpassed by photography. For the first time in the process of pictorial reproduction, photography freed the hand of the most important artistic functions which henceforth devolved only upon the eye looking into a lens. Since the eye perceives more swiftly than the hand can draw, the process of pictorial reproduction was accelerated so enormously that it could keep pace with speech. A film operator shooting a scene in the studio captures the images at the speed of an actor’s speech. Just as lithography virtually implied the illustrated newspaper, so did photography foreshadow the sound film. The technical reproduction of sound was tackled at the end of the last century. These convergent endeavors made predictable a situation which Paul Valery pointed up in this sentence:"),
	[]byte("Around 1900 technical reproduction had reached a standard that not only permitted it to reproduce all transmitted works of art and thus to cause the most profound change in their impact upon the public; it also had captured a place of its own among the artistic processes. For the study of this standard nothing is more revealing than the nature of the repercussions that these two different manifestations – the reproduction of works of art and the art of the film – have had on art in its traditional form."),
	[]byte("Even the most perfect reproduction of a work of art is lacking in one element: its presence in time and space, its unique existence at the place where it happens to be. This unique existence of the work of art determined the history to which it was subject throughout the time of its existence. This includes the changes which it may have suffered in physical condition over the years as well as the various changes in its ownership. The traces of the first can be revealed only by chemical or physical analyses which it is impossible to perform on a reproduction; changes of ownership are subject to a tradition which must be traced from the situation of the original."),
}

var BenjaminLines = [][]byte{
	[]byte("Because he never raises his eyes to the great and the meaningful, the philistine has taken experience as his gospel. It has become for him a message about life's commonness. But he has never grasped that there exists something other than experience, that there are values—inexperienceable—which we serve."),
	[]byte("Jede Äußerung menschlichen Geisteslebens kann als eine Art der Sprache aufgefaßt werden, und diese Auffassung erschließt nach Art einer wahrhaften Methode überall neue Fragestellungen."),
	[]byte("There is no muse of philosophy, nor is there one of translation."),
	[]byte("A religion may be discerned in capitalism—that is to say, capitalism serves essentially to allay the same anxieties, torments, and disturbances to which the so-called religions offered answers."),
	[]byte("Capitalism is presumably the first case of a blaming, rather than a repenting cult. ... An enormous feeling of guilt not itself knowing how to repent, grasps at the cult, not in order to repent for this guilt, but to make it universal, to hammer it into consciousness and finally and above all to include God himself in this guilt."),
}

func DefaultTestConfig(verbose bool) *config.Config {

	return &config.Config{
		Verbose:       verbose,
		ServerTimeout: 500,
		ClientTimeout: 500,
		LogFileMode:   0644,
		LogFile:       TmpLog(),
		MaxChunkSize:  1024 * 1024 * 2,
		PartitionSize: 1024 * 1024 * 2,
	}
}

func CheckGoldenFile(filename string, b []byte, golden bool) {
	goldenFile := "testdata/" + filename + ".golden"
	goldenActual := "testdata/" + filename + ".actual.golden"

	if golden {
		log.Printf("Writing golden file to %s", goldenFile)
		ioutil.WriteFile(goldenFile, b, 0644)
		return
	}

	expected, err := ioutil.ReadFile(goldenFile)
	CheckError(err)
	if !bytes.Equal(b, expected) {
		ioutil.WriteFile(goldenActual, b, 0644)
		log.Fatalf("Golden files didn't match: wrote output to %s", goldenActual)
	}
}

func CheckError(err error) {
	if err != nil {
		log.Printf("%s", debug.Stack())
		log.Fatalf("Unexpected error %v", err)
	}
}

func WaitForChannel(c chan struct{}) {
	select {
	case <-c:
	case <-time.After(500 * time.Millisecond):
		log.Printf("%s", debug.Stack())
		log.Fatalf("timed out waiting for receive on channel")
	}
}

func Repeat(s string, n int) string {
	b := bytes.Buffer{}
	for i := 0; i < n; i++ {
		b.WriteString(s)
		b.WriteString(" ")
	}
	return b.String()
}
