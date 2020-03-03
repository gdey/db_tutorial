package main_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gdey/db_tutorial/db"
)

type CheckFn func(t *testing.T, got []byte) (good bool)

type checkOutput []byte

func (co checkOutput) Check(t *testing.T, got []byte) bool {
	t.Helper()
	expected := []byte(co)
	if string(got) != string(expected) {
		t.Errorf("output, expected \n`%s`\ngot \n`%s`", expected, got)

		t.Logf("output, expected \n`%#v`\ngot \n`%#v`", expected, got)
		return false
	}
	return true

}

type checkLine string

func CheckOutputStrings(strs ...string) checkOutput {
	buff := new(bytes.Buffer)
	for i, str := range strs {
		// check to see if all strings but the last has \n at the end.
		if i != (len(strs)-1) && !strings.HasSuffix(str, "\n") {
			str = str + "\n"
		}
		buff.WriteString(str)
	}
	return checkOutput(buff.Bytes())
}

func (cl checkLine) Check(t *testing.T, got []byte) bool {
	t.Helper()
	lines := strings.Split(string(got), "\n")
	for _, l := range lines {
		if l == string(cl) {
			return true
		}
	}
	t.Errorf("find line, expected to find '%s', got did not find", string(cl))
	ldim := int(math.Log10(float64(len(lines))))
	for i, l := range lines {
		t.Logf(" %0*d : '%s'", ldim, i, l)
	}
	return false
}

func TestDatabase_Presistence(t *testing.T) {

	dir, err := ioutil.TempDir("", "dbtest")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir) // cleanup

	buff := new(bytes.Buffer)
	in := bytes.NewBuffer([]byte("insert 1 user1 person1@example.com\n.exit"))
	args := []string{os.Args[0], filepath.Join(dir, "test.db")}
	code := db.Main(buff, buff, in, args)
	if code != 0 {
		t.Errorf("exit code, expected 0 got %d", code)
		return
	}
	if !CheckOutputStrings("db > Executed.", "db > ").Check(t, buff.Bytes()) {
		return
	}
	buff.Reset()
	in = bytes.NewBuffer([]byte("select\n.exit"))
	code = db.Main(buff, buff, in, args)
	if code != 0 {
		t.Errorf("exit code, expected 0 got %d", code)
		return
	}
	if !CheckOutputStrings("db > (1, user1, person1@example.com)", "Executed.", "db > ").Check(t, buff.Bytes()) {
		return
	}
}

func TestDatabase(t *testing.T) {
	type tcase struct {
		inputs []byte
		check  CheckFn
		code   int
	}

	dir, err := ioutil.TempDir("", "dbtest")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir) // cleanup

	fn := func(tc tcase) func(*testing.T) {
		return func(t *testing.T) {
			buff := new(bytes.Buffer)
			in := bytes.NewBuffer(tc.inputs)
			args := []string{
				os.Args[0],
				filepath.Join(dir,
					filepath.Base(t.Name())+".db",
				),
			}
			code := db.Main(buff, buff, in, args)
			if tc.check != nil && !tc.check(t, buff.Bytes()) {
				return
			}
			if code != tc.code {
				t.Errorf("exit code, expected %d got %d", tc.code, code)
				return
			}
		}
	}

	tests := map[string]tcase{
		"inserts and retrieves as row": {
			inputs: []byte(`insert 1 user1 person1@example.com
select
.exit`),
			check: checkOutput(`db > Executed.
db > (1, user1, person1@example.com)
Executed.
db > `).Check,
			code: 0,
		},
		"prints error message when table is full": {
			inputs: func() []byte {
				var buff = new(bytes.Buffer)
				idim := int(math.Log10(float64(db.TableMaxRows + 1)))
				log.Printf("Adding %v rows", db.TableMaxRows)
				for i := 0; i < int(db.TableMaxRows+1); i++ {
					fmt.Fprintf(buff, "insert %[2]d user%0[1]*d person%0[1]*d@example.com\n", idim, i+1)
				}
				fmt.Fprintln(buff, ".exit")
				return buff.Bytes()
			}(),
			check: checkLine("db > Error: Table full.").Check,
			code:  0,
		},
		"allows inserting strings that are the maximum length": func() tcase {
			longUsername := strings.Repeat("a", db.ColumnUsernameSize)
			longEmail := strings.Repeat("a", db.ColumnEmailSize)
			input := []byte(fmt.Sprintf("insert 1 %s %s\nselect\n.exit", longUsername, longEmail))
			return tcase{
				inputs: input,
				code:   0,
				check: checkOutput([]byte(fmt.Sprintf(`db > Executed.
db > (1, %s, %s)
Executed.
db > `, longUsername, longEmail))).Check,
			}
		}(),
		"prints error message if strings are too long": func() tcase {
			longUsername := strings.Repeat("a", db.ColumnUsernameSize+1)
			longEmail := strings.Repeat("a", db.ColumnEmailSize+1)
			input := []byte(fmt.Sprintf("insert 1 %s %s\nselect\n.exit", longUsername, longEmail))
			return tcase{
				inputs: input,
				code:   0,
				check: checkOutput([]byte(`db > String is too long.
db > Executed.
db > `)).Check,
			}
		}(),
		"print an error message if id is negative": tcase{
			inputs: []byte("insert -1 gostack foo@bar.com\nselect\n.exit"),
			code:   0,
			check: checkOutput([]byte(`db > ID must be positive.
db > Executed.
db > `)).Check,
		},
	}
	for name, tc := range tests {
		t.Run(name, fn(tc))
	}
}
