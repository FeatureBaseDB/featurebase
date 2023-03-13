package cli

import (
	"strings"

	"github.com/pkg/errors"
)

// splitter is a line splitter which splits a line into queryParts and
// metaCommands. It may not be necessary to have this be a separate struct since
// it contains no members and just has the one `split()` method, but here we
// are.
type splitter struct {
	replacer *replacer
}

func newSplitter(r *replacer) *splitter {
	return &splitter{
		replacer: r,
	}
}

// split splits the given line into queryParts and metaCommands.
// If a metaCommand is found, everything after that is considered either arguments to that
// metaCommand, or additional metaCommands. In other words, queryParts can not follow
// metaCommands in the same line.
//
// A line can contain any of the following patterns:
// 1- [queryParts...]: "select * from tbl; select"
// 2- [metaCommands...]: "\! pwd \q"
// 3- [queryParts...][metaCommands...]: "select * from \i file.sql"
func (s *splitter) split(line string) ([]queryPart, []metaCommand, error) {
	// Look for a comment line.
	if strings.HasPrefix(line, "--") {
		return nil, nil, nil
	}

	// Look for a meta command.
	parts := strings.SplitN(line, `\`, 2)

	switch len(parts) {
	case 1:
		// slice of queryParts (pattern 1)
		if qps, err := s.splitQueryParts(strings.TrimSpace(parts[0])); err != nil {
			return nil, nil, errors.Wrap(err, "splitting query parts")
		} else {
			return qps, nil, nil
		}
	case 2:
		// slice of parts + slice of meta commands (pattern 3)
		// or
		// slice of meta commands (pattern 2)
		qps, err := s.splitQueryParts(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, nil, errors.Wrap(err, "splitting query parts")
		}

		mcs, err := s.splitMetaCommands(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, nil, errors.Wrap(err, "splitting meta commands")
		}

		return qps, mcs, nil
	}

	return nil, nil, nil
}

func (s *splitter) splitQueryParts(line string) ([]queryPart, error) {
	if line == "" {
		return nil, nil
	}

	// Look for a termination character;
	parts := strings.Split(line, terminationChar)

	// Do variable replacement.
	for i := range parts {
		parts[i] = s.replacer.replace(parts[i])
	}

	if len(parts) == 1 {
		part0 := strings.TrimSpace(parts[0])
		return []queryPart{
			newPartRaw(part0),
		}, nil
	}

	qps := make([]queryPart, 0)
	for i := range parts {
		part := strings.TrimSpace(parts[i])
		if part == "" {
			// If the line starts with a ";", treat it as a terminator for a
			// previous line.
			if i == 0 {
				qps = append(qps, &partTerminator{})
			}
			continue
		}
		qps = append(qps, newPartRaw(part))
		if i < len(parts)-1 {
			qps = append(qps, &partTerminator{})
		}
	}

	return qps, nil
}

func (s *splitter) splitMetaCommands(in string) ([]metaCommand, error) {
	parts := strings.Split(in, `\`)
	if len(parts) == 1 {
		mc, err := splitMetaCommand(parts[0], s.replacer)
		if err != nil {
			return nil, errors.Wrapf(err, "splitting meta command: %s", parts[0])
		}
		return []metaCommand{mc}, nil
	}

	mcs := make([]metaCommand, 0)
	for i := range parts {
		part := strings.TrimSpace(parts[i])
		if part == "" {
			continue
		}
		mc, err := splitMetaCommand(part, s.replacer)
		if err != nil {
			return nil, errors.Wrapf(err, "splitting meta command: %s", part)
		}
		mcs = append(mcs, mc)
	}

	return mcs, nil
}
