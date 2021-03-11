package server

import (
	"bytes"
	"errors"
	"log"
	"os/exec"
	"time"

	"github.com/DataIntelligenceCrew/OpenDataLink/internal/navigation"
)

func (s *Server) buildOrganization(name string, datasetIDs []string) error {
	start := time.Now()
	var err error
	s.organization, err = navigation.BuildOrganization(
		s.db, s.ft, s.organizationConfig, datasetIDs)
	if err != nil {
		return err
	}
	log.Printf("built organization %q in %v", name, time.Since(start).String())
	s.organization.SetRootName(name)

	dot, err := s.organization.MarshalDOT()
	if err != nil {
		return err
	}
	var out bytes.Buffer
	cmd := exec.Command("dot", "-Tsvg")
	cmd.Stdin = bytes.NewReader(dot)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return err
	}
	svg := out.Bytes()
	i := bytes.Index(svg, []byte("<svg"))
	if i < 0 {
		return errors.New("<svg not found")
	}
	svg = svg[i:]
	s.organizationGraphSVG = svg
	return nil
}
