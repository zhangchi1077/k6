/*
 *
 * k6 - a next-generation load testing tool
 * Copyright (C) 2017 Load Impact
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

//go:generate rice embed-go

package compiler

import (
	"sync"
	"time"

	rice "github.com/GeertJohan/go.rice"
	"github.com/dop251/goja"
	"github.com/dop251/goja/parser"
	"github.com/mitchellh/mapstructure"
	"github.com/sirupsen/logrus"
)

var (
	DefaultOpts = map[string]interface{}{
		"presets":       []string{"latest"},
		"ast":           false,
		"sourceMaps":    false,
		"babelrc":       false,
		"compact":       false,
		"retainLines":   true,
		"highlightCode": false,
	}

	once     sync.Once     // nolint:gochecknoglobals
	preprocs *preprocessor // nolint:gochecknoglobals
)

// CompatibilityMode specifies the JS compatibility mode
// nolint:lll
//go:generate enumer -type=CompatibilityMode -transform=snake -trimprefix CompatibilityMode -output compatibility_mode_gen.go
type CompatibilityMode uint8

const (
	// CompatibilityModeES6 is achieved with Babel and core.js
	CompatibilityModeES6 CompatibilityMode = iota + 1
	// CompatibilityModeES51 is standard goja
	CompatibilityModeES51
)

// A Compiler compiles JavaScript source code (ES5.1 or ES6) into a goja.Program
type Compiler struct{}

// New returns a new Compiler
func New() *Compiler {
	return &Compiler{}
}

// Preprocess the given code by compiling it to ES5 with Babel and pre-compiling
// core.js, while synchronizing to ensure only a single preprocessor instance
// is in use.
func (c *Compiler) Preprocess(src, filename string) (code string, srcmap SourceMap, err error) {
	var pp *preprocessor
	if pp, err = newPreprocessor(); err != nil {
		return
	}

	pp.mutex.Lock()
	defer pp.mutex.Unlock()
	return pp.babelTransform(src, filename)
}

// Compile the program
func (c *Compiler) Compile(src, filename string, pre, post string,
	strict bool, compatMode CompatibilityMode) (*goja.Program, *goja.Program, string, error) {
	var preProgram *goja.Program
	code := pre + src + post
	ast, err := parser.ParseFile(nil, filename, code, 0)
	if err != nil {
		if compatMode == CompatibilityModeES6 {
			codeProcessed, _, errP := c.Preprocess(src, filename)
			if errP != nil {
				return nil, nil, code, errP
			}
			return c.Compile(codeProcessed, filename, pre, post, strict, compatMode)
		}
		return nil, nil, src, err
	}
	if compatMode == CompatibilityModeES6 && preprocs != nil {
		preProgram = preprocs.corejs
	}
	pgm, err := goja.CompileAST(ast, strict)
	return pgm, preProgram, code, err
}

type preprocessor struct {
	vm        *goja.Runtime
	babel     goja.Value
	corejs    *goja.Program
	transform goja.Callable
	mutex     sync.Mutex //TODO: cache goja.CompileAST() in an init() function?
}

func newPreprocessor() (*preprocessor, error) {
	var err error

	once.Do(func() {
		conf := rice.Config{
			LocateOrder: []rice.LocateMethod{rice.LocateEmbedded},
		}
		// Compile Babel
		babelSrc := conf.MustFindBox("lib").MustString("babel.min.js")
		vm := goja.New()
		if _, err = vm.RunString(babelSrc); err != nil {
			return
		}

		// Compile core.js for use in VU Goja runtimes
		corejs := goja.MustCompile(
			"corejs.min.js",
			conf.MustFindBox("lib").MustString("corejs.min.js"),
			true,
		)

		babel := vm.Get("Babel")
		bObj := babel.ToObject(vm)
		preprocs = &preprocessor{vm: vm, babel: babel, corejs: corejs}
		if err = vm.ExportTo(bObj.Get("transform"), &preprocs.transform); err != nil {
			return
		}
	})

	return preprocs, err
}

// Transform the given code into ES5 with Babel
func (pp *preprocessor) babelTransform(src, filename string) (code string, srcmap SourceMap, err error) {
	opts := DefaultOpts
	opts["filename"] = filename

	startTime := time.Now()
	v, err := pp.transform(pp.babel, pp.vm.ToValue(src), pp.vm.ToValue(opts))
	if err != nil {
		return
	}
	logrus.WithField("t", time.Since(startTime)).Debug("Babel: Transformed")

	vO := v.ToObject(pp.vm)
	if err = pp.vm.ExportTo(vO.Get("code"), &code); err != nil {
		return
	}
	var rawmap map[string]interface{}
	if err = pp.vm.ExportTo(vO.Get("map"), &rawmap); err != nil {
		return
	}
	if err = mapstructure.Decode(rawmap, &srcmap); err != nil {
		return
	}
	return
}
